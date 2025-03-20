import asyncio
import datetime
import io
import os
import queue

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from oedi_etl.utils import parse_s3_url
DECIMAL_PLACES = 7  #! this is way precise. The data has much higher floating point precision than physically realistic. Highly unlikely that the precisions are possible so round them to save space.


class ETLWorker:
    """
    ETLWorker handles the full ETL pipeline:
    - Fetches the file from S3.
    - If bypass=False → Transforms the data.
    - Uploads the transformed or bypassed data to S3.
    - Uses ETLTracker to record progress.
    - Listens to a shared queue for tasks ([job_name, dest(dest_bucket + output_dir), s3_url, bypass]).
    - Exits gracefully when receiving a shutdown signal ([None, None, None, None]).
    """

    def __init__(self, task_queue, log_queue, tracking_queue):
        self.worker_id = os.getpid()
        self.label = f"[ETLWorker {self.worker_id}]"
        self.task_queue = task_queue  # Shared task queue
        self.log_queue = log_queue  # Shared logging queue
        self.tracking_queue = tracking_queue  # Shared tracking queue
        self.s3_client = boto3.client("s3")

    def _log(self, level, message):
        """Push logs to the log queue instead of direct logging."""
        self.log_queue.put((level, f"{self.label} {message}"))

    async def _fetch_file_from_s3(self, job_name, s3_url, bypass):
        """Fetches the file as a stream from S3 and records progress."""
        try:
            src_bucket, object_key = parse_s3_url(s3_url)
            response = await asyncio.to_thread(
                self.s3_client.get_object, Bucket=src_bucket, Key=object_key
            )
            self._log("info", f"Fetched {object_key} from {src_bucket}.")

            # Track fetch event via tracking queue
            event_type = "metadata_fetched" if bypass else "data_fetched"
            self.tracking_queue.put((job_name, object_key, event_type))

            return response["Body"]
        except ClientError as e:
            self._log(
                "error", f"Failed to fetch {object_key} from {src_bucket}: {str(e)}"
            )
            return None

    def _transform_data(self, table):
        """
        Transforms the Parquet table into hourly intervals, applying mean/min aggregations
        dynamically based on the available schema.

        - Aggregates 15-minute data into 1-hour intervals.
        - Dynamically detects float columns and rounds them to 2 decimals.
        - Works with generic column structures without hardcoded field names.

        Args:
            table (pyarrow.Table): Input Parquet table.

        Returns:
            pyarrow.Table: Transformed and aggregated table.
        """
        try:
            MILLISECONDS_IN_AN_HOUR = 3600 * 1000
            timestamp_column = table["timestamp"]

            # Convert timestamps to hourly bins
            floored_timestamps = []
            for chunk in timestamp_column.chunks:
                floored_chunk = [
                    int(ts.timestamp() * 1000)
                    // MILLISECONDS_IN_AN_HOUR
                    * MILLISECONDS_IN_AN_HOUR
                    for ts in chunk.to_pylist()
                ]
                floored_timestamps.append(
                    pa.array(
                        [
                            datetime.datetime.fromtimestamp(
                                ts / 1000, tz=datetime.timezone.utc
                            )
                            for ts in floored_chunk
                        ]
                    )
                )

            # Create new timestamp column
            floored_timestamps_array = pa.chunked_array(floored_timestamps)
            table = table.remove_column(table.schema.get_field_index("timestamp"))
            table = table.append_column("timestamp", floored_timestamps_array)

            # Identify aggregation rules dynamically
            group_by_columns = (
                ["timestamp", "bldg_id"]
                if "bldg_id" in table.column_names
                else ["timestamp"]
            )
            aggregation_rules = []

            for col in table.column_names:
                if col in group_by_columns:
                    aggregation_rules.append(
                        (col, "min")
                    )  # Use 'min' to retain grouping keys
                elif pa.types.is_floating(table.schema.field(col).type):
                    aggregation_rules.append((col, "mean"))  # Average float values
                else:
                    aggregation_rules.append(
                        (col, "min")
                    )  # Default to min for categorical data

            # Perform aggregation
            grouped_table = table.group_by(group_by_columns)
            transformed_table = grouped_table.aggregate(aggregation_rules)

            # Round floating point numbers
            for field in transformed_table.schema:
                if pa.types.is_floating(field.type):
                    transformed_table = transformed_table.set_column(
                        transformed_table.schema.get_field_index(field.name),
                        field.name,
                        pc.round(transformed_table[field.name], DECIMAL_PLACES),
                    )

            return transformed_table

        except Exception as e:
            self._log("error", f"Transformation failed: {str(e)}")
        return None

    async def _upload_to_s3(self, job_name, dest_bucket, object_key, bypass, data):
        """Uploads transformed or bypassed data to the destination bucket and records it correctly."""
        try:
            await asyncio.to_thread(
                self.s3_client.put_object,
                Bucket=dest_bucket,
                Key=object_key,
                Body=data,
            )
            self._log("info", f"Uploaded {object_key} to {dest_bucket}.")

            # Track upload event via tracking queue
            event_type = "metadata_uploaded" if bypass else "data_uploaded"
            self.tracking_queue.put((job_name, object_key, event_type))

        except ClientError as e:
            self._log("error", f"Upload failed for {object_key}: {str(e)}")

    async def _etl_job(self, job_name, dest, s3_url, bypass):
        """Runs the full ETL pipeline: fetch → {transform | bypass} → upload."""
        self._log("info", f"Starting ETL for {s3_url} (bypass={bypass})")

        file_stream = await self._fetch_file_from_s3(job_name, s3_url, bypass)
        if not file_stream:
            return

        reader = pq.ParquetFile(io.BytesIO(file_stream.read()))
        _, object_key = parse_s3_url(s3_url)
        if bypass:
            raw_metadata = file_stream.read()  # Directly transfer metadata
            self.tracking_queue.put((job_name, object_key, "metadata_bypassed"))
        else:
            table = reader.read()  # Read entire Parquet table
            transformed_table = self._transform_data(table)

            if transformed_table is None:
                self._log("error", f"Transformation failed for {s3_url}.")
                return

            # Write transformed table to buffer
            sink = io.BytesIO()
            pq.write_table(transformed_table, sink, compression="snappy")
            transformed_bytes = sink.getvalue()

            # Track transformation event via tracking queue
            self.tracking_queue.put((job_name, object_key, "data_transformed"))

        # Write final transformed/bypassed data to a buffer
        if bypass:
            etl_output = raw_metadata  # Direct transfer for metadata
        else:
            sink = io.BytesIO()
            pq.write_table(
                pq.read_table(io.BytesIO(transformed_bytes)), sink, compression="snappy"
            )
            etl_output = sink.getvalue()

        # prep output keys
        if "/" not in dest:
            raise ValueError(
                f"Invalid dest format: '{dest}'. Expected 'bucket-name/path'"
            )
        parts = dest.split("/", 1)
        dest_bucket = parts[0]
        out_dir = f"{parts[1]}/{job_name}"
        dest_object_key = f"{out_dir}/{object_key}"

        await self._upload_to_s3(job_name, dest_bucket, dest_object_key, bypass, etl_output)
        self._log("info", f"Completed ETL job for {s3_url}. (bypass={bypass})")

    async def run(self):
        """Continuously processes tasks from the queue until receiving a poison pill,
        then performs a clean shutdown."""
        self._log("info", "Worker started, waiting for tasks...")
        while True:
            try:
                job_name, dest_bucket, s3_url, bypass = await asyncio.to_thread(
                    self.task_queue.get_nowait
                )
            except queue.Empty:
                await asyncio.sleep(1)  # Avoid busy-waiting if queue is empty
                continue

            # Check for poison pill (shutdown signal)
            if (
                job_name is None
                and dest_bucket is None
                and s3_url is None
                and bypass is None
            ):
                self._log("info", "Poison pill received. Exiting worker loop...")
                break

            await self._etl_job(job_name, dest_bucket, s3_url, bypass)


def worker_process(task_queue, log_queue, tracking_queue):
    """
    Top-level function that initializes and runs an ETLWorker.
    This function is necessary because multiprocessing requires picklable functions.
    """
    try:
        worker = ETLWorker(task_queue, log_queue, tracking_queue)
        asyncio.run(worker.run())  # Start the async worker loop
    except Exception as e:
        log_queue.put(
            ("error", f"[worker_manager] Worker {os.getpid()} failed: {str(e)}")
        )
        raise e
