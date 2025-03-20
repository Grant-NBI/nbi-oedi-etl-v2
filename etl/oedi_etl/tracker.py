import asyncio
import hashlib
import json
import sys
import time
from queue import Empty

from oedi_etl.log import get_logger
from oedi_etl.utils import get_tracking_key

logger = get_logger("ETLTracker")
DRAIN_SECONDS = 5


class ETLTracker:
    """
    ETLTracker: A Singleton Class for Tracking ETL Progress

    This class manages the tracking of ETL jobs using an in-memory queue-based system.
    It logs the transition states of both data and metadata files as they move through
    the ETL pipeline, ensuring accurate tracking of missing files, transformations,
    and uploads.

    ### **Key Responsibilities**
    1. **Tracking Events:**
    - Monitors the states of data and metadata files (indexed, fetched, transformed, uploaded).
    - Uses an internal hash-based tracking system to avoid redundant operations.

    2. **Discrepancy Detection:**
    - Identifies missing fetches, transformations, bypasses, and uploads.
    - Reports discrepancies separately for data and metadata.

    3. **Drain Loop for Shutdown:**
    - Ensures all tracking events are processed before ETL shutdown.
    - Waits for any remaining jobs before closing.
    - Settles logs before generating the final summary.

    4. **Final ETL Summary Logging:**
    - Generates a structured JSON summary of processed and missing files.
    - Logs only meaningful discrepancies to reduce noise.

    ### **Core Components**
    - **Tracking Hashes:** Stores file transitions in `tracking_data` dictionary.
    - **Queue-Based Processing:** Uses an async `run_drain_loop` to process events.
    - **Discrepancy Calculator:** Differentiates between missing fetches, transformations, bypasses, and uploads.

    """

    _instance = None  # Singleton instance

    def __new__(cls, tracking_queue=None):
        """Ensure only one instance exists, and optionally set queue."""
        if cls._instance is None:
            cls._instance = super(ETLTracker, cls).__new__(cls)
            cls._instance.tracking_queue = tracking_queue  # Assign queue if provided
            cls._instance.tracking_data = {}
            cls._instance.start_time = time.time()

        return cls._instance

    def set_queue(self, tracking_queue):
        """Ensures queue is initialized when set externally."""
        self.tracking_queue = tracking_queue

    async def run_drain_loop(self, shutdown_event, drain_timeout=10):
        """Processes tracking events from the queue and ensures full draining before shutdown."""
        logger.info("[ETLTracker] Started draining tracking queue...")

        last_activity_time = time.time()
        try:
            while True:
                try:
                    job_name, object_key, event_type = self.tracking_queue.get_nowait()
                    if object_key is None:
                        logger.warning("Object key is empty. Maybe a poison pill?")
                        continue  # Skip this message
                    self._track_event(job_name, object_key, event_type)
                    last_activity_time = (
                        time.time()
                    )  # Reset last activity time when an event is processed

                except Empty:
                    # If shutdown event is set and queue is empty, start final drain countdown
                    if (
                        shutdown_event.is_set()
                        and time.time() - last_activity_time >= drain_timeout
                    ):
                        logger.info(
                            f"[ETLTracker] Final drain timeout reached ({drain_timeout}s). Exiting..."
                        )
                        break  # Exit the loop only when we have waited long enough after last activity once shutdown is set

                    await asyncio.sleep(0.5)
            logger.info("[ETLTracker] Tracking queue fully drained and shutting down.")
        except Exception as e:
            logger.error(f"[ETLTracker] Error in draining loop: {e}")
            raise e
        finally:
            await asyncio.sleep(DRAIN_SECONDS)  # settle logs
            await self._print_summary()
            self.tracking_data.clear()

    def _track_event(self, job_name, object_key, event_type):
        if not self.tracking_queue:
            raise RuntimeError("ETLTracker queue is not initialized!")

        # Ensure job entry exists
        if job_name not in self.tracking_data:
            self.tracking_data[job_name] = {
                "data_listed_hash": set(),
                "data_fetched_hash": set(),
                "data_transformed_hash": set(),
                "data_uploaded_hash": set(),
                "metadata_listed_hash": set(),
                "metadata_fetched_hash": set(),
                "metadata_bypassed_hash": set(),
                "metadata_uploaded_hash": set(),
                "hash_to_file": {},
            }

        # Generate file hash
        file_hash = self._hash_object_key(object_key)
        self.tracking_data[job_name]["hash_to_file"][file_hash] = object_key

        # Assign event to the correct state
        if event_type == "data_indexed":
            self.tracking_data[job_name]["data_listed_hash"].add(file_hash)
        elif event_type == "data_fetched":
            self.tracking_data[job_name]["data_fetched_hash"].add(file_hash)
        elif event_type == "data_transformed":
            self.tracking_data[job_name]["data_transformed_hash"].add(file_hash)
        elif event_type == "data_uploaded":
            self.tracking_data[job_name]["data_uploaded_hash"].add(file_hash)

        elif event_type == "metadata_indexed":
            self.tracking_data[job_name]["metadata_listed_hash"].add(file_hash)
        elif event_type == "metadata_fetched":
            self.tracking_data[job_name]["metadata_fetched_hash"].add(file_hash)
        elif event_type == "metadata_bypassed":
            self.tracking_data[job_name]["metadata_bypassed_hash"].add(file_hash)
        elif event_type == "metadata_uploaded":
            self.tracking_data[job_name]["metadata_uploaded_hash"].add(file_hash)

        self._log_event(event_type, job_name, object_key)

    def _log_event(self, event_type, job_name, object_key):
        """Generates structured logging messages based on event type."""
        messages = {
            "data_indexed": f"[{job_name}] Indexed DATA file: {object_key}",
            "data_fetched": f"[{job_name}] Fetched DATA file from S3: {object_key}",
            "data_transformed": f"[{job_name}] Transformed DATA file: {object_key}",
            "data_uploaded": f"[{job_name}] Uploaded DATA file: {object_key}",
            "metadata_indexed": f"[{job_name}] Indexed METADATA file: {object_key}",
            "metadata_fetched": f"[{job_name}] Fetched METADATA file from S3: {object_key}",
            "metadata_bypassed": f"[{job_name}] Bypassed METADATA file: {object_key}",
            "metadata_uploaded": f"[{job_name}] Uploaded METADATA file: {object_key}",
        }
        logger.info(
            messages.get(
                event_type,
                f"[{job_name}] Unknown event '{event_type}' for {object_key}",
            ),
        )

    def add_object(self, job_name, object_key, bypass=False):
        """Adds an S3 object to tracking and increments counters while tracking hashes separately for metadata and data."""
        event_type = "metadata_indexed" if bypass else "data_indexed"
        self._track_event(job_name, object_key, event_type)

    def _record_data_obj_fetch(self, job_name, object_key):
        """Marks a data object as fetched."""
        self._track_event(job_name, object_key, "data_fetched")

    def _record_data_obj_upload(self, job_name, object_key):
        """Marks a data object as uploaded and removes its hash from tracking."""
        self._track_event(job_name, object_key, "data_uploaded")

    def _record_metadata_obj_fetch(self, job_name, object_key):
        """Marks a metadata object as fetched."""
        self._track_event(job_name, object_key, "metadata_fetched")

    def _record_metadata_obj_upload(self, job_name, object_key):
        """Marks a metadata object as uploaded and removes its hash from tracking."""
        self._track_event(job_name, object_key, "metadata_uploaded")

    def _record_transformed(self, job_name, object_key):
        """Marks a file as transformed."""
        self._track_event(job_name, object_key, "data_transformed")

    def _record_bypassed(self, job_name, object_key):
        """Marks a metadata file as bypassed and tracks its hash."""
        self._track_event(job_name, object_key, "metadata_bypassed")

    def _calculate_discrepancies(
        self, listed, fetched, transformed_or_bypassed, uploaded, hash_to_file
    ):
        """
        Calculates discrepancies for a given category (either data or metadata).

        Args:
            listed (set): Files that were listed.
            fetched (set): Files that were fetched.
            transformed_or_bypassed (set): Files that were transformed (for data) or bypassed (for metadata).
            uploaded (set): Files that were uploaded.
            hash_to_file (dict): Mapping of file hashes to actual file paths.

        Returns:
            dict: A dictionary containing missing fetches, missing transforms/bypasses, and missing uploads.
        """
        missing_fetches = listed - fetched
        missing_transforms_or_bypasses = fetched - transformed_or_bypassed
        missing_uploads = transformed_or_bypassed - uploaded

        return {
            "missing_fetches": [
                hash_to_file.get(h, f"Unknown Hash: {h}") for h in missing_fetches
            ],
            "missing_transforms_or_bypasses": [
                hash_to_file.get(h, f"Unknown Hash: {h}")
                for h in missing_transforms_or_bypasses
            ],
            "missing_uploads": [
                hash_to_file.get(h, f"Unknown Hash: {h}") for h in missing_uploads
            ],
        }

    def _detect_discrepancies(self):
        """
        Detects missing transitions separately for both metadata and data processing pipelines.

        Returns:
            dict: Mapping job names to separate missing transitions for metadata and data.
        """
        discrepancies = {}

        for job_name, job_data in self.tracking_data.items():
            discrepancies[job_name] = {"data": {}, "metadata": {}}

            # Extract hash sets for this job
            data_listed = job_data.get("data_listed_hash", set())
            data_fetched = job_data.get("data_fetched_hash", set())
            data_transformed = job_data.get("data_transformed_hash", set())
            data_uploaded = job_data.get("data_uploaded_hash", set())

            metadata_listed = job_data.get("metadata_listed_hash", set())
            metadata_fetched = job_data.get("metadata_fetched_hash", set())
            metadata_bypassed = job_data.get("metadata_bypassed_hash", set())
            metadata_uploaded = job_data.get("metadata_uploaded_hash", set())

            hash_to_file = job_data["hash_to_file"]

            # **Calculate Discrepancies for Data**
            data_discrepancies = self._calculate_discrepancies(
                data_listed, data_fetched, data_transformed, data_uploaded, hash_to_file
            )
            discrepancies[job_name]["data"]["missing_fetches"] = data_discrepancies[
                "missing_fetches"
            ]
            discrepancies[job_name]["data"]["missing_transforms"] = data_discrepancies[
                "missing_transforms_or_bypasses"
            ]
            discrepancies[job_name]["data"]["missing_uploads"] = data_discrepancies[
                "missing_uploads"
            ]

            # **Calculate Discrepancies for Metadata**
            metadata_discrepancies = self._calculate_discrepancies(
                metadata_listed,
                metadata_fetched,
                metadata_bypassed,
                metadata_uploaded,
                hash_to_file,
            )
            discrepancies[job_name]["metadata"]["missing_fetches"] = (
                metadata_discrepancies["missing_fetches"]
            )
            discrepancies[job_name]["metadata"]["missing_bypasses"] = (
                metadata_discrepancies["missing_transforms_or_bypasses"]
            )
            discrepancies[job_name]["metadata"]["missing_uploads"] = (
                metadata_discrepancies["missing_uploads"]
            )

        # **Remove jobs with no discrepancies**
        return {
            job: issues
            for job, issues in discrepancies.items()
            if any(issues["data"].values()) or any(issues["metadata"].values())
        }

    async def _print_summary(self):
        """
        Logs final ETL job summary, ensuring accurate tracking of missing files.

        This function identifies and reports discrepancies in data and metadata processing
        by analyzing missing transitions (fetches, transforms, bypasses, uploads).

        Summary breakdown:
        - Total listed vs. uploaded files for both data and metadata.
        - Separate count of missing fetches, transforms, bypasses, and uploads.
        - Filters out any category with no discrepancies to reduce log noise.
        - Ensures logs are fully flushed before exit.

        Fixes:
        - Properly distinguishes between data transforms and metadata bypasses.
        - Filters out empty lists instead of logging them.
        - Ensures missing files are correctly categorized.

        Returns:
            None (Logs the summary directly).
        """

        end_time = time.time()
        elapsed_time = round(end_time - self.start_time, 2)

        # Get all discrepancies
        discrepancies = self._detect_discrepancies()

        # Helper function to filter out empty lists
        def filter_non_empty(dictionary, key):
            """Removes empty lists from log output."""
            return {job: data[key] for job, data in dictionary.items() if data[key]}

        # Initialize summary structure
        summary = {
            "time_stat": {"total_time_seconds": elapsed_time},
            "data_files_stats": {
                "total_data_files_listed": sum(
                    len(job["data_listed_hash"]) for job in self.tracking_data.values()
                ),
                "total_data_files_uploaded": sum(
                    len(job["data_uploaded_hash"])
                    for job in self.tracking_data.values()
                ),
                "missing_data_fetches_count": sum(
                    len(v["data"].get("missing_fetches", []))
                    for v in discrepancies.values()
                ),
                "missing_data_transforms_count": sum(
                    len(v["data"].get("missing_transforms", []))
                    for v in discrepancies.values()
                ),
                "missing_data_uploads_count": sum(
                    len(v["data"].get("missing_uploads", []))
                    for v in discrepancies.values()
                ),
                "missing_data_fetches_files": filter_non_empty(discrepancies, "data"),
                "missing_data_transforms_files": filter_non_empty(
                    discrepancies, "data"
                ),
                "missing_data_uploads_files": filter_non_empty(discrepancies, "data"),
            },
            "metadata_files_stats": {
                "total_metadata_files_listed": sum(
                    len(job["metadata_listed_hash"])
                    for job in self.tracking_data.values()
                ),
                "total_metadata_files_uploaded": sum(
                    len(job["metadata_uploaded_hash"])
                    for job in self.tracking_data.values()
                ),
                "missing_metadata_fetches_count": sum(
                    len(v["metadata"].get("missing_fetches", []))
                    for v in discrepancies.values()
                ),
                "missing_metadata_bypasses_count": sum(
                    len(v["metadata"].get("missing_bypasses", []))
                    for v in discrepancies.values()
                ),
                "missing_metadata_uploads_count": sum(
                    len(v["metadata"].get("missing_uploads", []))
                    for v in discrepancies.values()
                ),
                "missing_metadata_fetches_files": filter_non_empty(
                    discrepancies, "metadata"
                ),
                "missing_metadata_bypasses_files": filter_non_empty(
                    discrepancies, "metadata"
                ),
                "missing_metadata_uploads_files": filter_non_empty(
                    discrepancies, "metadata"
                ),
            },
        }

        logger.info("[ETLTracker] ETL Job Summary:")
        logger.info(json.dumps(summary, indent=4))

        # Ensure logs are flushed and settled
        await asyncio.sleep(2)
        sys.stdout.flush()
        sys.stderr.flush()

    def _hash_object_key(self, object_key):
        """Generates a compact hash for an object key, ensuring proper encoding."""
        if not isinstance(object_key, str):
            raise TypeError(
                f"Invalid type for object_key: {type(object_key)}. Expected str."
            )

        # Get last 6 partitions (tracking key)
        tracking_key = get_tracking_key(object_key)

        # Encode in UTF-8 (preserving slashes for uniqueness)
        return hashlib.blake2b(tracking_key.encode("utf-8"), digest_size=8).hexdigest()

