import asyncio

import boto3

s3 = boto3.client("s3")


async def list_files_for_partition(bucket, prefix, max_keys, logger=None):
    """
    Lists all files under the given S3 prefix using pagination.

    - Runs the paginator inside `run_in_executor` to avoid blocking the event loop.
    - Introduces periodic yielding to prevent event loop starvation.

    Args:
        bucket (str): Name of the S3 bucket.
        prefix (str): S3 prefix under which to list files.
        max_keys (int): Maximum number of keys to retrieve per page.
        logger (structlog.BoundLogger): Job-specific logger.

    Returns:
        List of full S3 URLs.
    """
    paginator = s3.get_paginator("list_objects_v2")

    async def paginate_s3():
        """Async function to paginate S3 listing"""
        file_urls = []
        page_iterator = await asyncio.to_thread(
            lambda: paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
        )

        for page in page_iterator:
            if "Contents" in page:
                file_urls.extend(
                    f"s3://{bucket}/{obj['Key']}" for obj in page["Contents"]
                )

            # Sleep to prevent excessive CPU load
            await asyncio.sleep(0)

        return file_urls

    logger.debug(f"[list_files_for_partition] Listing files under: {prefix}")
    return await paginate_s3()


def get_relative_metadata_s3_prefix(
    relative_metadata_prefix_type, state, upgrade, counties=None
):
    """
    Generates metadata directory paths and filenames dynamically based on metadata version.
    Supports multiple counties.

    Args:
        relative_metadata_prefix_type (str): Metadata version ("1.0" or "2.0").
        state (str): The state code (e.g., "AK").
        upgrade (int or str): The upgrade level.
        counties (list, optional): List of county codes (for county-level metadata).

    Returns:
        list[str]: A list of full metadata file paths relative to the metadata root.
    """
    upgrade_str = "baseline" if str(upgrade) == "0" else f"upgrade{int(upgrade):02}"

    if relative_metadata_prefix_type == "1":
        # Version 1: State-level metadata
        return [
            f"{state}_{upgrade_str}_metadata_and_annual_results.parquet/by_state/state={state}/parquet"
        ]
        # version 2: County-level metadata
    elif relative_metadata_prefix_type == "2":
        if counties:
            return [
                f"by_state_and_county/full/parquet/state={state}/county={county}/{state}_{county}_{upgrade_str}.parquet"
                for county in counties
            ]

        # version 3: Aggregated county level metadata
    elif relative_metadata_prefix_type == "3":
        if counties:
            return [
                f"by_state_and_county/full/parquet/state={state}/county={county}/{state}_{county}_{upgrade_str}_agg.parquet"
                for county in counties
            ]
        else:
            return [f"by_state/state={state}/parquet/{state}_{upgrade_str}_agg.parquet"]
    else:
        raise ValueError(
            f"Invalid relative_metadata_prefix_type: {relative_metadata_prefix_type}"
        )


async def list_metadata_files_in_s3(config, logger):
    """
    Retrieves metadata files from S3 by checking direct file existence.

    - Supports Version 1 (state-level metadata), Version 2 (county-level metadata), and Version 3 (aggregated metadata).
    - Uses correct iteration over states, counties, and upgrades.
    - Ensures fair execution by yielding control.

    Args:
        config (dict): ETL configuration.
        logger (structlog.BoundLogger): Job-specific logger.

    Returns:
        List[str]: A list of metadata file keys found in S3.
    """
    src_bucket = config["src_bucket"]
    metadata_root_dir = config["metadata_root_dir"].rstrip("/")
    relative_metadata_prefix_type = config["relative_metadata_prefix_type"]
    state = config["state"]
    upgrades = config["upgrades"]

    # Determine counties
    counties = config.get("counties", [])
    if counties == ["*"]:
        counties = await list_all_counties(src_bucket, metadata_root_dir, state, logger)

    # Get metadata file paths
    metadata_files = []
    metadata_partitions = []

    for upgrade in upgrades:
        metadata_partitions.extend(
            get_relative_metadata_s3_prefix(
                relative_metadata_prefix_type, state, upgrade, counties
            )
        )

    try:
        logger.debug("[indexer.py] Starting metadata file retrieval...")

        for metadata_partition in metadata_partitions:
            file_key = f"{metadata_root_dir}/{metadata_partition}"
            metadata_files.append(f"s3://{file_key}" if file_key.startswith(src_bucket) else f"s3://{src_bucket}/{file_key}")
            await asyncio.sleep(0)  # Yield execution

        logger.info(
            f"[indexer.py] Metadata file retrieval completed. Total files found: {len(metadata_files)}"
        )
        return metadata_files

    except Exception as e:
        logger.error(f"[indexer.py] Error retrieving metadata files: {e}")
        return []


async def list_all_counties(bucket, path, state, logger):
    """
    Lists all counties under path.
    Returns a list of county codes found in the directory structure.
    """
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = await asyncio.to_thread(
        lambda: paginator.paginate(Bucket=bucket, Prefix=path, Delimiter="/")
    )

    counties = []
    for page in page_iterator:
        if "CommonPrefixes" in page:
            for prefix_obj in page["CommonPrefixes"]:
                county_folder = prefix_obj["Prefix"].split("/")[-2]
                county_code = county_folder.replace("county=", "")
                counties.append(county_code)

    if not counties:
        logger.warning(
            f"[indexer.py] No counties found under {path}. Skipping county-level metadata."
        )
        return []

    logger.debug(
        f"[indexer.py] Found {len(counties)} counties for state {state}: {counties}"
    )
    return counties


async def list_data_files_in_s3(config, logger):
    """
    Lists S3 data files using async pagination.

    - Lists partitions dynamically based on config.
    - Uses an async paginator to fetch files efficiently.
    - Ensures fair execution by yielding control.

    Args:
        config (dict): Job configuration.
        logger (structlog.BoundLogger): Job-specific logger.

    Returns:
        List[str]: A list of data file keys found in S3.
    """
    base_partition = config["base_partition"]
    data_partition_in_release = config["data_partition_in_release"]
    release_name = config["release_name"]
    release_year = config["release_year"]
    listing_page_size = int(config.get("listing_page_size", 500))
    src_bucket = config["src_bucket"]
    state = config["state"]
    upgrades = config["upgrades"]

    partitions = [
        f"{base_partition}/{release_year}/{release_name}/{data_partition_in_release}/upgrade={upgrade}/state={state}"
        for upgrade in upgrades
    ]

    data_files = []

    try:
        logger.info(
            f"[indexer.py] Starting data file listing for {len(partitions)} partitions..."
        )

        for partition in partitions:
            logger.debug(f"[indexer.py] Listing data files in partition: {partition}")

            file_keys = await list_files_for_partition(
                src_bucket, partition, listing_page_size, logger
            )

            if not file_keys:
                logger.warning(
                    f"[indexer.py] No data files found in partition: {partition}"
                )
                continue

            data_files.extend(file_keys)

            await asyncio.sleep(0)  # Yield execution

        logger.info(
            f"[indexer.py] Data file listing completed. Total files found: {len(data_files)}"
        )
        return data_files

    except Exception as e:
        logger.error(f"[indexer.py] Error listing data files: {e}")
        return []


async def list_objects(config, logger):
    """
    Unified entry point for listing metadata and data files.
    - Fetches metadata and data files **concurrently**.
    - Yields **individual file keys** instead of entire lists.
    - Marks metadata files as `bypass=True` and data files as `bypass=False`.

    Args:
        config (dict): Job configuration.
        logger (structlog.BoundLogger): Job-specific logger.

    Yields:
        (str, bool): File key and bypass flag.
    """
    # Run metadata and data file listing concurrently
    metadata_files, data_files = await asyncio.gather(
        list_metadata_files_in_s3(config, logger),
        list_data_files_in_s3(config, logger),
    )

    # Yield files as they are discovered
    for object_key in metadata_files:
        yield object_key, True  # bypass=True
    for object_key in data_files:
        yield object_key, False  # bypass=False
