"""
ETL and Metadata Job Orchestrator

This module coordinates the execution of ETL jobs and metadata copying tasks,
managing both workloads efficiently while ensuring seamless integration with a pool of ETL workers.

### Architectural Overview:
1. **ETL & Metadata Handling**:
   - ETL jobs handle structured data transformation and ingestion.
   - Metadata copying ensures that relevant metadata files are moved alongside data.
   - Both are processed in a unified workflow with tracking per job.

2. **Parallel Job Execution**:
   - Each job is handled independently based on its configuration.
   - Jobs are indexed separately and processed concurrently.

3. **Worker-Based Execution**:
   - A dedicated WorkerManager is responsible for managing worker processes.
   - Workers consume tasks from a shared queue and process one file at a time.

4. **Tracking and Monitoring**:
   - ETLTracker records progress, including data transformations, uploads, and metadata copies.
   - Jobs are tracked individually to maintain per-job statistics.

5. **AWS Glue Integration**:
   - Once processing completes, Glue Crawlers are updated to discover new partitions for querying.

"""

import os
from datetime import datetime

import boto3

from oedi_etl.indexer import list_objects
from oedi_etl.log import get_logger
from oedi_etl.manager import WorkerManager
from oedi_etl.utils import get_job_name, update_glue_crawlers

glue = boto3.client("glue")
logger = get_logger("main.py")


async def etl_main(etl_config):
    """Orchestrates ETL process across multiple jobs."""
    # Per Glue job run
    output_dir = (
        f"{etl_config['output_dir']}/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    )
    dest_bucket = etl_config.get(
        "output_bucket",
        os.getenv("OUTPUT_BUCKET_NAME") or os.getenv("TEST_BUCKET_NAME"),
    )

    if not dest_bucket:
        raise ValueError("Destination bucket not provided.")

    # ETL settings
    settings = etl_config["settings"]
    num_workers = etl_config["settings"].get("num_workers", os.cpu_count() * 2)
    idle_timeout_in_minutes = settings.get("idle_timeout_in_minutes", 5)
    listing_page_size = settings.get("listing_page_size", 500)
    max_listing_queue_size = settings.get("max_listing_queue_size", 1000)

    # Initialize WorkerManager (handles workers & shared state)
    worker_manager = WorkerManager(num_workers)

    # Start Workers
    worker_manager.start()

    # Process Jobs Independently
    data_partitions_to_crawl = {}
    metadata_partitions_to_crawl = {}
    total_jobs = 0

    for index, job_config in enumerate(etl_config["job_specific"]):
        job_name = get_job_name(job_config, index)
        _logger = get_logger(job_name)
        _logger.info(f"[main.py] Starting job: {job_name}")
        data_partitions_to_crawl[job_name] = set()
        metadata_partitions_to_crawl[job_name] = set()

        # insert shared configurations
        config = {
            "base_partition": etl_config["base_partition"],
            "counties": job_config.get("counties", []),
            "data_partition_in_release": etl_config["data_partition_in_release"],
            "dest_bucket": dest_bucket,
            "idle_timeout_in_minutes": idle_timeout_in_minutes,
            "listing_page_size": listing_page_size,
            "max_listing_queue_size": max_listing_queue_size,
            "metadata_root_dir": job_config["metadata_root_dir"],
            "relative_metadata_prefix_type": job_config[
                "relative_metadata_prefix_type"
            ],
            "output_dir": output_dir,
            "release_name": job_config["release_name"],
            "release_year": job_config["release_year"],
            "src_bucket": etl_config["src_bucket"],
            "state": job_config["state"],
            "upgrades": job_config["upgrades"],
        }

        # Index & Dispatch Tasks
        async for object_key, bypass in list_objects(config, _logger):
            dest = f"{dest_bucket}/{output_dir}"
            worker_manager.add_object(job_name, dest, object_key, bypass)

            total_jobs += 1

            # Track partitions per job for Glue Crawlers
            partition = os.path.dirname(object_key)
            if bypass:
                metadata_partitions_to_crawl[job_name].add(partition)
            else:
                data_partitions_to_crawl[job_name].add(partition)

        logger.info(f"[main.py] Finished indexing {total_jobs} objects for {job_name}.")

    # Inject Poison Pill AFTER all jobs are indexed
    for _ in range(num_workers):
        worker_manager.add_object(None, None, None, None)

    # Wait for all workers to finish before running Glue Crawlers
    await worker_manager.wait_for_completion()

    # Update Glue Crawlers only AFTER all jobs are done
    await update_glue_crawlers(
        dest_bucket,
        output_dir,
        {job: partitions for job, partitions in data_partitions_to_crawl.items()},
        {job: partitions for job, partitions in metadata_partitions_to_crawl.items()},
    )
