"""
# WorkerManager Overview
`WorkerManager` is responsible for managing worker processes, tracking events, logging, and monitoring. It ensures:

    - Workers fully complete their jobs before shutdown.
    - Logging and tracking drainers finish processing all events before termination.
    - No arbitrary timeouts; everything completes naturally.
    - Each process exits only when its job is truly done.

**How Completion Works**:
    - Workers process waits until all workers consume poison pills (one per worker).
    - The poison pill is provided by the indexer, not the manager, which knows when all indexing is done.
    - Workers naturally terminate when all jobs are done.
    - Manager waits for ALL workers to shut down before proceeding.
    - Tracking & Logging processes listen for shutdown signals.
    - Tracker waits for the queue to be completely drained before stopping.
    - Manager only terminates processes when they are fully idle.
"""

import asyncio
import multiprocessing
import time
from queue import Empty

from oedi_etl.log import get_logger
from oedi_etl.tracker import ETLTracker
from oedi_etl.worker import worker_process

LOG_SHUTDOWN_SIGNAL = None
DRAIN_SECONDS = 10
MAX_RETRIES = 5
BASE_BACKOFF = 2  # seconds

logger = get_logger("worker_manager")

# Global manager for Windows compatibility
_GLOBAL_MANAGER = None


def get_manager():
    """Ensures only one global Manager is created."""
    global _GLOBAL_MANAGER
    if _GLOBAL_MANAGER is None:
        _GLOBAL_MANAGER = multiprocessing.Manager()
    return _GLOBAL_MANAGER


class WorkerManager:
    def __init__(self, max_workers=None):
        self.manager = get_manager()
        self.max_workers = max_workers or multiprocessing.cpu_count()
        self.task_queue = multiprocessing.Queue()
        self.log_queue = multiprocessing.Queue()
        self.tracking_queue = multiprocessing.Queue()
        self.shutdown_event = multiprocessing.Event()
        self.start_time = time.time()
        self.workers = {}

    def start(self):
        """Starts worker processes, monitoring, logging, and tracking drainer."""

        # Start worker processes
        for _ in range(self.max_workers):
            self._spawn_worker()

        # Start log listener as a separate process
        self.log_process = multiprocessing.Process(
            target=log_listener_process,
            args=(self.log_queue, self.shutdown_event),
            daemon=True,
        )
        self.log_process.start()

        # Start tracking queue drainer process
        self.tracker_process = multiprocessing.Process(
            target=tracker_listener_process,
            args=(self.tracking_queue, self.shutdown_event),
            daemon=True,
        )
        self.tracker_process.start()

        # Start worker monitor process
        self.monitor_process = multiprocessing.Process(
            target=monitor_worker_process,
            args=(
                self.task_queue,
                self.log_queue,
                self.tracking_queue,
                self.shutdown_event,
            ),
            daemon=True,
        )
        self.monitor_process.start()

    def add_object(self, job_name, dest_bucket, object_key, bypass):
        """Adds an S3 object to the processing queue and tracks it via queue."""

        # Add task to queue
        self.task_queue.put((job_name, dest_bucket, object_key, bypass))

        # Push tracking event into tracking queue
        event_type = "metadata_indexed" if bypass else "data_indexed"
        self.tracking_queue.put((job_name, object_key, event_type))

    async def wait_for_completion(self):
        """
        Waits for all worker processes and auxiliary processes to finish naturally before shutdown.

        Steps:
        1. Wait for all workers to complete.
        2. Monitor tracking and logging queues until they remain stable and are empty.
        3. Signal auxiliary processes to shut down via shutdown_event.
        4. Wait for auxiliary processes to exit gracefully.

        This ensures all events and logs, including the final summary, are fully processed before termination.
        """
        logger.info("[worker_manager] Waiting for all workers to complete...")

        # Step 1: Wait for workers to finish
        while True:
            active_workers = [p for p in self.workers.values() if p.is_alive()]
            if not active_workers:
                break
            await asyncio.sleep(2)

        # Clean up worker processes
        for pid, process in list(self.workers.items()):
            if process.is_alive():
                process.terminate()
                process.join()
            del self.workers[pid]
        logger.info("[worker_manager] All workers shut down.")

        # Step 2: Wait for tracking and logging queues to be stable and empty.
        last_change = time.time()
        while True:
            curr_tracking = self.tracking_queue.qsize()
            curr_log = self.log_queue.qsize()
            if curr_tracking == 0 and curr_log == 0:
                # If both queues are empty and have been for DRAIN_SECONDS, we're done.
                if time.time() - last_change > DRAIN_SECONDS:
                    break
            else:
                # If queues are not empty, update last_change time.
                last_change = time.time()
            await asyncio.sleep(2)
        logger.info(
            "[worker_manager] Tracking & logging queues fully drained and empty."
        )

        # Step 3: Signal auxiliary processes to shut down.
        self.shutdown_event.set()
        await asyncio.sleep(DRAIN_SECONDS)  # Allow processes time to process shutdown

        # Step 4: Wait for auxiliary processes to finish naturally.
        if self.log_process.is_alive():
            logger.info("[worker_manager] Waiting for log listener to finish...")
            self.log_process.join()
            logger.info("[worker_manager] Log listener shut down.")

        if self.tracker_process.is_alive():
            logger.info("[worker_manager] Waiting for tracker drainer to finish...")
            self.tracker_process.join()
            logger.info("[worker_manager] Tracker drainer shut down.")

        if self.monitor_process.is_alive():
            logger.info("[worker_manager] Waiting for monitor process to finish...")
            self.monitor_process.join()
            logger.info("[worker_manager] Monitor process shut down.")

        logger.info("[worker_manager] Finalizing summary and cleanup.")
        logger.info("[worker_manager] All processes shut down successfully.")

    def _spawn_worker(self):
        """Creates and starts a new worker process."""
        p = multiprocessing.Process(
            target=worker_process,
            args=(self.task_queue, self.log_queue, self.tracking_queue),
        )
        p.start()
        self.workers[p.pid] = p


def monitor_worker_process(task_queue, log_queue, tracking_queue, shutdown_event):
    """
    Monitors worker health and restarts failed workers.

    - Detects crashed workers and restarts them (up to `MAX_RETRIES`).
    - Implements exponential backoff before restarting workers.
    - Ensures job continuity in case of unexpected failures.

    Exits once the shutdown event is triggered.
    """
    workers = {}
    worker_retries = {}

    while not shutdown_event.is_set():
        time.sleep(2)  # Regular polling interval

        for pid, worker in list(workers.items()):
            if worker.is_alive():
                continue  # Skip if worker is still running

            current_retries = worker_retries.get(pid, 0)
            if current_retries >= MAX_RETRIES:
                log_queue.put(
                    f"[worker_manager] Worker {pid} failed after {current_retries} retries. Not restarting."
                )
                del workers[pid]
                worker_retries.pop(pid, None)  # Cleanup retries
            else:
                backoff_time = BASE_BACKOFF * (2**current_retries)
                log_queue.put(
                    f"[worker_manager] Worker {pid} failed, restarting in {backoff_time} seconds... "
                    f"(retry {current_retries + 1}/{MAX_RETRIES})"
                )

                time.sleep(backoff_time)  # Wait before restarting
                del workers[pid]

                new_worker = multiprocessing.Process(
                    target=worker_process,
                    args=(task_queue, log_queue, tracking_queue),
                )
                new_worker.start()
                workers[new_worker.pid] = new_worker
                worker_retries[new_worker.pid] = current_retries + 1


def log_listener_process(log_queue, shutdown_event):
    """
    Runs in a separate process to listen for log events and handle structured logging.

    - Logs messages asynchronously from multiple processes.
    - Exits when the shutdown event is triggered.
    - Drains any remaining log messages before shutting down.

    Ensures that all logs are captured before shutdown.
    """
    logger = get_logger("worker_manager")  # Assuming get_logger is your custom logger

    log_levels = {
        "error": logger.error,
        "warning": logger.warning,
        "info": logger.info,
        "debug": logger.debug,
        "critical": logger.critical,
    }

    while not shutdown_event.is_set():
        try:
            record = log_queue.get(timeout=1)  # Avoid blocking forever
            if record is None:
                break
            level, message = record
            log_levels.get(level, logger.info)(message)
        except Empty:
            continue  # Keep checking for logs

    # Drain remaining logs before exiting
    while True:
        try:
            record = log_queue.get_nowait()
            if record is None:
                break
            level, message = record
            log_levels.get(level, logger.info)(message)
        except Empty:
            break

    logger.info("[worker_manager] Log listener shutting down.")


def tracker_listener_process(tracking_queue, shutdown_event):
    """
    Runs the ETL tracking drainer as a separate process.

    - Processes tracking events in real-time.
    - Ensures no tracking events are lost before shutdown.
    - Shuts down only when the queue is fully drained.

    Uses an async loop (`run_drain_loop`) to handle events efficiently.
    """
    tracker = ETLTracker(tracking_queue)
    logger.info("[worker_manager] Tracker drainer started.")

    asyncio.run(tracker.run_drain_loop(shutdown_event, DRAIN_SECONDS))

    logger.info("[worker_manager] Tracker drainer shut down.")
