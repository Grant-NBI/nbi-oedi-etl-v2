import asyncio
import json
import multiprocessing
import os
import re
import signal
import sys

import boto3

from oedi_etl.log import get_logger

glue = boto3.client("glue")
logger = get_logger("utils.py")
s3 = boto3.client("s3")


async def manage_back_pressure(queue, max_queue_size):
    """
    Ensures that the queue does not exceed its maximum allowed size before processing continues.
    """
    if queue.qsize() >= max_queue_size:
        logger.info(
            f"[utils.py] Queue size ({queue.qsize()}) exceeds limit ({max_queue_size}). Pausing..."
        )
        await asyncio.sleep(2)


async def fetch_file_from_s3(src_bucket, object_key):
    """
    Fetches a file from an S3 bucket as a stream asynchronously.

    Args:
        src_bucket (str): The name of the source S3 bucket.
        object_key (str): The key (path) of the file/object to fetch from the S3 bucket.

    Returns:
        StreamingBody: The streaming body of the fetched file.
    """
    try:
        logger.debug(f"[utils.py] Fetching file: {object_key} ...")

        response = await asyncio.to_thread(
            s3.get_object, Bucket=src_bucket, Key=object_key
        )

        logger.debug(f"[utils.py] Fetching file: {object_key} ...")
        return response["Body"]  # StreamingBody object
    except Exception as e:
        logger.error(f"[utils.py] Error fetching file {object_key}: {e}")
        return None


async def upload_to_s3(dest_bucket, object_key, object_stream):
    """
    Uploads a file to S3 using a stream.

    Args:
        dest_bucket (str): The name of the destination S3 bucket.
        object_key (str): The key (path) where the file should be stored.
        object_stream: The streaming body to upload.
    """
    try:
        logger.debug(f"[utils.py] Uploading file: {object_key} ...")
        await asyncio.to_thread(
            s3.upload_fileobj, object_stream, dest_bucket, object_key
        )
        logger.info(f"[utils.py] Uploaded file: {object_key}")
    except Exception as e:
        logger.error(f"[utils.py] Error uploading file {object_key}: {e}")


def get_job_name(config, index):
    """
    Generates a unique job name based on the configuration.

    - Uses `release_name` and `release_year` as the base.
    - If `state` is provided, appends it.
    - Appends the index position to ensure uniqueness.

    Args:
        config (dict): Job configuration.
        index (int): Position of the job in the job list.

    Returns:
        str: Generated job name.
    """
    job_name_parts = [config["release_name"], config["release_year"]]

    if "state" in config and config["state"]:
        job_name_parts.append(config["state"])

    job_name_parts.append(f"job{index}")  # Append index for uniqueness

    return "_".join(job_name_parts)


async def update_glue_crawlers(
    dest_bucket, output_dir, data_partitions, metadata_partitions
):
    """
    Updates Glue Crawlers with new partitions for data and metadata.

    Args:
        dest_bucket (str): The destination S3 bucket.
        output_dir (str): The output directory.
        data_partitions (list): List of data partitions to crawl.
        metadata_partitions (list): List of metadata partitions to crawl.
    """
    output_prefix = f"s3://{dest_bucket}/{output_dir}"

    if data_partitions:
        glue.update_crawler(
            Name=os.getenv("DATA_CRAWLER_NAME"),
            Targets={
                "S3Targets": [
                    {"Path": f"{output_prefix}/{partition}"}
                    for partition in data_partitions
                ]
            },
            Configuration=json.dumps(
                {"Version": 1.0, "Grouping": {"TableLevelConfiguration": 11}}
            ),
        )

    if metadata_partitions:
        glue.update_crawler(
            Name=os.getenv("METADATA_CRAWLER_NAME"),
            Targets={
                "S3Targets": [
                    {"Path": f"{output_prefix}/{partition}", "Exclusions": ["*/csv/*"]}
                    for partition in metadata_partitions
                ]
            },
            Configuration=json.dumps(
                {"Version": 1.0, "Grouping": {"TableLevelConfiguration": 11}}
            ),
        )


async def force_shutdown(worker_manager):
    """
    Force shutdown:
    - Cancels all running async tasks safely.
    - Terminates lingering multiprocessing workers.
    - Ensures a clean system exit.
    """
    try:
        logger.info("[main.py] Initiating Forced Shutdown Process...")

        # Shutdown workers
        worker_manager.shutdown_workers()

        logger.info("[main.py] Cancelling all tasks...")
        current_task = asyncio.current_task()
        pending_tasks = {
            task
            for task in asyncio.all_tasks()
            if task is not current_task and not task.done()
        }

        for task in pending_tasks:
            task.cancel()

        # Allow cancellation to propagate
        await asyncio.gather(*pending_tasks, return_exceptions=True)

        logger.info("[main.py] All Tasks Canceled...")

        # Terminate lingering subprocesses
        for process in multiprocessing.active_children():
            if process.is_alive():
                logger.warning(f"[main.py] Terminating process {process.pid}")
                process.terminate()
                process.join(timeout=2)

                if process.is_alive():
                    logger.warning(
                        f"[main.py] Process {process.pid} did not terminate. Sending SIGKILL."
                    )
                    os.kill(process.pid, signal.SIGKILL)

        logger.info("[main.py] Shutdown process complete.")
    except Exception as cleanup_error:
        logger.error(f"[main.py] Cleanup encountered an error: {cleanup_error}")
    finally:
        logger.info("[main.py] Exiting system.")
        sys.exit(1)

def parse_s3_url(s3_url: str) -> tuple[str, str]:
    """
    Extracts the bucket name and object key from an S3 URL.

    Args:
        s3_url (str): The full S3 URL (e.g., "s3://bucket-name/path/to/object").

    Returns:
        tuple[str, str]: A tuple containing the bucket name and object key.
    """
    match = re.match(r"^s3://([^/]+)/(.+)", s3_url)
    if match:
        return match.group(1), match.group(2)
    raise ValueError(f"Invalid S3 URL: {s3_url}")

def get_tracking_key(object_key: str) -> str:
    """
    Extracts the last 6 partitions from an object key to generate a tracking key.

    Args:
        object_key (str): The object key (e.g., "comstock_amy2018_release_2/timeseries_individual_buildings/by_state/upgrade=1/state=AK/117185-1.parquet").

    Returns:
        str: The tracking key, which consists of the last 6 partitions.
    """
    parts = object_key.strip('/').split('/')
    return '/' + '/'.join(parts[-6:]) if len(parts) >= 6 else '/' + object_key
