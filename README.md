# NBI Open Energy Data Initiative (OEDI) - ETL and Querying infrastructure

## Quick Guide

### Deployment

The deployment process is automated using AWS CDK.

1. [Edit config](/config.json). Ensure the account, region, profile, and the ETL input parameters are correctly set.
2. [Edit SQL file](/sql/saved-queries.sql)
3. [Edit ETL file](/etl/oedi_etl/main.py)
4. Run `npm run deploy` from the project root to deploy the stack using the `config.json`.
5. Run `npm run workflow` from the project root to run Glue Job Workflow. This will orchestrate an ETL workflow, extract all the configured partitions from the configured bucket, transform them, upload them to the destination bucket, trigger crawlers to update the update the schemas in the Glue database, and copy the pertinent metadata.
6. Run `poetry run test` from the root of the ETL project `/etl` to run the ETL from the local machine. This is only useful for development and doesn't trigger the crawlers.

### `config.json`

* Functions as the configurator for both the `oedi_etl` and the `cdk/deployment` config. The `oedi_etl` receives a `config.etl_config` during the workflow run. This configuration is stored as part of the deployment. The `npm run workflow` will override this configuration per each invocation with the latest `config.etl_config`. However, any run via the console is only aware of the config available during the deployment unless manually input. Note that the deployment directly configures the Glue job instead of the workflow, and during the `npm run workflow`, the configuration is passed to the workflow, which relays it to the Glue job.

* *Note that this file is ignored by git ignore, and you need to create your own from the following example and save it to `config.json` at the root of the project workspace*.

### **Versions and Metadata Handling**

This system supports three metadata versions based on how building-level results are structured and aggregated:

---

* **Version `1.0` (Legacy)**
  * Uses **state-level, unaggregated** metadata.
  * One file per upgrade per state, containing all building results.
  * Stored under: `by_state/state={state}/parquet/`
  * Filename format:

    `{state}_{upgrade_str}_metadata_and_annual_results.parquet`

---

* **Version `2.0` (County-Specific)**
  * Uses **per-county unaggregated** metadata files.
  * Stored under: `by_state_and_county/full/parquet/state={state}/county={county}/`
  * Filename format:

    `{state}_{county}_{upgrade_str}.parquet`

  * Supports:
    * **Explicit county list** via `counties: ["G0601150", ...]`
    * **Wildcard fetch** via `counties: ["*"]` to dynamically retrieve all counties in a given state.

---

* **Version `3.0` (State-Level Aggregated from Counties)**
  * Contains **pre-aggregated results at the state level**, derived from county-level data.
  * Not suitable for per-county analysis or wildcard county selection.
  * Stored under: `by_state/state={state}/parquet/`
  * Filename format:

    `{state}_{upgrade_str}_agg.parquet`

  * ⚠️ **Note**: `counties` key must not be set for this version. This version is meant for summarized views only and does not support county resolution or fetch logic.

---

The ETL automatically resolves file paths, formats, and metadata logic based on the `relative_metadata_prefix_type` and optional `counties` field provided in the configuration.

---

### **Output Configuration**

* The ETL output is written to a configurable S3 directory under the staging/analytics bucket.
* The directory structure is environment-based, ensuring that different environments (e.g., dev, prod) are kept separate.
* All ETL output is written to a single table, ensuring that the querying process is streamlined. The table is automatically updated by the Glue Crawler after each ETL run.

### **Sample Configuration**

```json
{
  "monorepoRoot": ".",
  "deploymentConfig": [
    {
      "appName": "NbiBuildingAnalytics",
      "account": "653991346912",
      "deploymentEnv": "dev",
      "profile": "awsist-dev",
      "regions": ["us-west-2"],
      "requireApproval": "never",
      "glueJobTimeoutMinutes": 240
    }
  ],
  "etl_config": {
    "glue_job_timeout": 14400,
    "output_dir": "etl_output",
    "src_bucket": "oedi-data-lake",
    "data_partition_in_release": "timeseries_individual_buildings/by_state",
    "base_partition": "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock",
    "job_specific": [
      {
        "metadata_root_dir": "oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_1/metadata_and_annual_results",
        "relative_metadata_prefix_type": "1",
        "release_name": "comstock_amy2018_release_1",
        "release_year": "2024",
        "state": "AK",
        "upgrades": ["0"]
      },
      {
        "counties": ["G0200130", "G0200160", "G0200200"],
        "metadata_root_dir": "oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results",
        "relative_metadata_prefix_type": "2",
        "release_name": "comstock_amy2018_release_2",
        "release_year": "2024",
        "state": "AK",
        "upgrades": ["1"]
      },
      //!BUG:  list_all_counties have some bug and this should not be used till it is fixed:instead enumerate all the counties you need or use the aggregates (type 3)
      // {
      //   "counties": ["*"],
      //   "metadata_root_dir": "oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results",
      //   "relative_metadata_prefix_type": "2",
      //   "release_name": "comstock_amy2018_release_2",
      //   "release_year": "2024",
      //   "state": "AK",
      //   "upgrades": ["1"]
      // },
      {
        "metadata_root_dir": "oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results_aggregates",
        "relative_metadata_prefix_type": "3",
        "release_name": "comstock_amy2018_release_2",
        "release_year": "2024",
        "state": "AK",
        "upgrades": ["1"]
      }
    ],
    "settings": {
      "log_dir": "logs",
      "log_filename": "etl.log",
      "logging_level": "DEBUG",
      "idle_timeout_in_minutes": 5,
      "listing_page_size": 100,
      "max_listing_queue_size": 500
    }
  }
}


```

---

## Design Guide

### Key Considerations

* **Data Structure Assumptions**: ETL assumes the base partition and data structure follow specific conventions. Other schemas may not be compatible without modification.
* **Flexible Jobs**: `job_specific` settings allow processing different releases, but the overall data structure is fixed.
* **Deployment**: Deployment settings ensure the app is deployed in the correct AWS environment with no manual approval needed.

The ETL workflow is designed to handle configurable data extraction, transformation, and load (ETL) operations and provide an efficient querying mechanism that interfaces with PowerBI.

By separating the ETL process from post-ETL querying, the system ensures optimal performance and cost-efficiency while maintaining flexibility for future modifications. The JSON-based configuration allows easy adaptation to changing data needs, while the infrastructure built on CDK ensures that the deployment is scalable and manageable.

### ETL

NOTE: *You can directly use Athena with the DL and pretty much achieve the same thing. The ETL is used for query performance and cost effectiveness given the project setup. Also, running the join during the ETL is inefficient for storage purposes. This is done to optimize the querying performance and deemed acceptable given the project use case*

The ETL script is written in Python and is manually triggered with configurable input parameters, such as `release_year`, `release_name`, `state`, and `upgrades`. There are two parts to the script. One handles each configuration set corresponding to a single task and lists and filters S3 objects based on these parameters, fetching only the relevant data. As the script processes the filtered objects, it performs transformations such as aggregating from 15-minute intervals to 1-hour intervals, compressing the data using Snappy, and joining relevant metadata. The transformed data is then loaded into the staging area in S3. This process is optimized for parallel execution, utilizing in-memory transformations and uploading data efficiently to minimize latency and maximize resource usage. Then there is another part of the script that runs multiple tasks in parallel.

The ETL is executed once per project setup for a given time period, while the Athena querying is used for ongoing data exploration and feeding the BI platform.

#### Data Lake Structure

The root data for this ETL process is stored in the following bucket:
`https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F`

Bucket structure:
`oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/`

#### Configurable Parameters

1. **Bucket and Prefix**:
   * These parameters should be configurable to handle various data sources.

2. **Release Year**:
   * Example directories: `2024/`, `2023/`, `2022/`, etc.

3. **Release Name**:
   * Example directories:
     * `comstock_amy2018_release_1/`
     * `resstock_amy2018_release_2/`
     * `resstock_dataset_2024.1/`
     * ...

4. **Data Directory (renamed to `data_block`)**:
   * This represents different sections within each release.
   * Examples of directories:
     * `building_energy_models/`
     * `geographic_information/`
     * `metadata/`
     * `timeseries_aggregates/`
     * `timeseries_individual_buildings/`
     * `weather/`
   * The structures within these directories may vary, and the ETL is designed to handle `timeseries_individual_buildings` data. Making it more configurable is possible but requires more development time and is outside the scope of this project.

5. **Partitioning by Region/State**:
   * In the `timeseries_individual_buildings` directory, the data is further divided into:
     * `by_puma_midwest/`
     * `by_puma_na/`
     * `by_puma_northeast/`
     * `by_puma_south/`
     * `by_puma_west/`
     * `by_state/`
     * `by_state_and_county/`

   * For this project, the focus is on the `by_state` and `by_state_and_county` partitions:
     * `by_state`: Contains **aggregated** data at the state level.
     * `by_state_and_county`: Contains data **partitioned by county**.
   * The ETL process automatically determines whether to process **state-level** or **county-level** data based on the configuration:
     * If `counties` **is not provided**, the ETL processes **by_state**.
     * If `counties` **contains specific county codes**, the ETL processes **by_state_and_county** for those counties.
     * If `counties` **is set to `["*"]`**, the ETL dynamically lists and processes **all counties** under `by_state_and_county`.

   * The `by_state` and `by_state_and_county` directories are partitioned as:
     * `upgrade=0`, `upgrade=1`, `upgrade=2`, etc.
     * `state=AK`, `state=AL`, `state=CA`, etc.
     * If county-level metadata is used, `county={county_code}` is also included.

   * The ETL should extract specific sets of `upgrades`, `states`, and optionally `counties`, and these values must be configurable.

6. **Metadata**:
   * The metadata, located in the `metadata_and_annual_results` or `metadata_and_annual_results_aggregates` directories, provides building-specific information.
   * Metadata is stored in three formats depending on the version:

     * **Version 1.0 (Legacy, State-Level Only)**:
       * Metadata is partitioned under `by_state` and follows the format:

       ```md
       CA_baseline_metadata_and_annual_results.parquet
       CA_upgrade01_metadata_and_annual_results.parquet
       CA_upgrade02_metadata_and_annual_results.parquet
       ```

     * **Version 2.0 (Current, State and County Level)**:
       * **State-Level Metadata (No `counties` key present)**
         * Stored in: `by_state/full/parquet/`
         * Example:

         ```md
         state=CA/CA_baseline.parquet
         state=CA/CA_upgrade01.parquet
         ```

       * **County-Level Metadata (Explicit `counties` key present)**
         * Stored in: `by_state_and_county/full/parquet/`
         * Example:

         ```md
         state=CA/county=G0600010/CA_G0600010_baseline.parquet
         state=CA/county=G0600020/CA_G0600020_upgrade01.parquet
         ```

       * **Wildcard County Fetch (`counties: ["*"]`)**
         * All counties are dynamically retrieved and processed under `by_state_and_county`.

     * **Version 3.0 (Aggregated County Metadata by State)**:
       * Stores **aggregated** county-level metadata at the **state level**.
       * Stored in: `by_state/parquet/`
       * Example:

       ```md
       state=CA/CA_baseline_agg.parquet
       state=CA/CA_upgrade01_agg.parquet
       ```

   * The ETL automatically determines the correct metadata version, structure, and fetch logic based on the configuration.

*Note: You can configure multiple ETL jobs for different releases, states, or granularities (state vs. county) as needed.*

### System Overview

The ETL process is automated as part of a Glue Workflow, which orchestrates the entire ETL process across an AWS Glue job and metadata and data crawlers. This can be scheduled but as per requirement it's manually triggered.

### Key Steps

1. **Glue Workflow Orchestration**:
   The workflow begins by invoking the main Glue ETL Job, which performs the extraction, transformation, and loading of the data. When this job status transitions to "SUCCEEDED", the Glue Crawlers are triggered to update the schema in the Glue Data Catalog.

2. **ETL Glue Job**:
   The primary Glue ETL job processes the input data and stores the results in the S3 output bucket. After the job finishes, it updates the partitions it processed as s3 targets to the Glue Crawler

3. **Glue Crawlers**:
   The crawlers ( Metadata and data crawlers) are triggered to automatically discover the schema and new partitions from the output data in the S3 bucket. This ensures that the Glue Data Catalog stays up-to-date, making the data available for querying.

4. **Athena Querying**:
   Once the Glue Crawler completes its updates, Athena is ready to query the data. The workflow integrates with Athena by making the processed data queryable through Athena WorkGroups and saved queries, which are available for BI tools like PowerBI.

### ETL Job Overview

This ETL system is the core component of the pipeline and is packaged as a Python poetry-based wheel (`.whl`) for deployment and reuse.

---

### Legacy Architecture (Old System)

The previous system was built with **modular task-based orchestration**, where:

* **Fetch**, **Transform**, and **Upload** were treated as **independent pipeline stages**.
* These stages were connected via async worker pools with separate task queues for each stage.
* Metadata was read and bypassed during the transformation stage.
* The transformation logic included optional partitioning and allowed custom filtering, column selection, and rounding rules. The main crux of the architecture was to allow evolving the transformation phase.
* Logging was centralized across processes and handled via an inter-process queue.
* A monitor and crawler updater coordinated the final output summary and partition sync with Glue.

---

### New Architecture (Current System)

The system has been **fully redesigned** for better maintainability, flexibility, and alignment with evolving metadata formats (and possible future changes in metadata or data formats).

Key changes and improvements:

#### Simplified ETL Execution Model

* Each file (data or metadata) is processed independently by a **single worker** from start to finish (fetch → transform → upload).
  * These workers run via **Python multiprocessing** to utilize available CPU cores with async I/O
  * Task distribution is handled through a centralized queue that feeds complete ETL jobs to available workers.

#### Decoupled Indexing

* The **Indexer** is now a dedicated async component that handles:
  * **Discovery of data and metadata files** via paginated S3 listings.
  * **Metadata versioning logic**: Dynamically generates file paths based on config and version type (v1, v2, v3).
  * Separation of metadata and data logic from ETL execution itself.
  * Most importantly, it's easier now to handle future changes in metadata and data file structures.

#### File-Type Awareness & Independent Tracking

* **Metadata and data files are now treated and tracked separately** throughout the pipeline.
  * Each file type has its own indexing, fetch, transform (if needed), upload, and logging lifecycle.
  * This design allows **metadata schema/version changes** to be isolated to the indexer + configuration, without ETL code rewrites.

#### Centralized Logging (via a logging queue)

* Logging continues to use a dedicated process, fed by all workers via a shared log queue.
* Ensures structured and complete capture of worker status, transformations, warnings, and errors.

#### Improved Tracking & Summary

* The **ETLTracker** now provides finer-grained tracking and discrepancy detection:
  * Distinguishes between data and metadata transitions.
  * Validates fetch, transform, and upload steps independently.
  * Emits a final job summary with detailed insights into missing transitions or failed states.

---

### Module Breakdown (Unchanged from Summary)

| Component       | Role                                                                 |
|----------------|----------------------------------------------------------------------|
| **`main.py`**   | Entry point. Orchestrates ETL jobs, manages configuration, invokes workers, updates Glue crawlers. |
| **`manager.py`** | Manages lifecycle of worker processes, logging, and tracking. Ensures clean shutdown and coordination. |
| **`worker.py`**  | Executes ETL pipeline: fetch → (transform/bypass) → upload. Tracks job states via queues. |
| **`indexer.py`** | Indexes S3 data & metadata files using async paginated S3 listings. |
| **`log.py`**     | Provides structured logging for local and AWS Glue environments. |
| **`utils.py`**   | Utilities for S3 interaction, Glue crawler updates, job naming, shutdown, etc. |
| **`tracker.py`** | Tracks ETL job state transitions and reports discrepancies in processing. |

---

### Design Principles

* **Single-responsibility workers** handle one file end-to-end to reduce cross-cutting complexity.
* **Async S3 operations** prevent event loop starvation during listing, fetches, or uploads.
* **Multiprocessing** allows full CPU core utilization for Parquet transforms.
* **Unified task queue** replaces stage-specific queues, simplifying orchestration.
* **Dynamic metadata support** makes it easy to onboard format changes with minimal code change.

---

*This refactor was driven primarily by changing metadata formats and the need for better separation of responsibilities between indexing and processing logic. The result is a faster, more flexible, and easier-to-maintain system.*

### Crawlers: Schema Discovery

* After the ETL job completes,  Glue Crawlers are triggered by the workflow to automatically discover the schema of the output data. One crawler target data and another one target metadata each writing to separate tables in the Glue Data Catalog.
* The Crawlers update the Athena tables schema with new partitions or newly added data.
* The respective S3 Targets are updated by the  by the glue job to reflect the latest partitions processed.

## Athena Integration

The ETL pipeline outputs data to an S3 bucket, which is directly queried by Athena using the schema discovered by the crawlers.

### Querying Process

* **Workgroup Setup**: A dedicated Athena Workgroup is configured for the project. This includes saved queries for frequent data operations.
* **Saved Queries**: Common queries (e.g., aggregating data, filtering by building type) are stored as saved queries, allowing for quick access to frequently run operations.
* **Efficient Querying**: The use of Parquet format and the aggregation of data in the ETL step ensures minimal data scanning, lowering query costs and improving performance.
* **Shared Bucket**: A single S3 bucket is used for both **ETL outputs** and **query results**. This bucket is divided into two directories: `etl-outputs` and `query-results`.

### Athena Query Integration with PowerBI

* Athena provides the querying interface to PowerBI. The saved queries are used to expose clean data views to the BI platform, allowing for visualization without extensive manual querying.

## Key Infrastructure Components

The following components are implemented using AWS CDK (note that this is the infrastructure that support the app system, and the cdk itself is an app that deploy the etl app)

1. **Glue Workflow**:
   * The main orchestrator that triggers the ETL process and the Glue Crawler sequentially, ensuring the end-to-end automation of the ETL process.

2. **ETL Job**:
    * Manually triggered by the Glue Workflow, driven by an editable Python script.
    * Responsible for extraction, transformation, and loading of data (see ETL Flow Overview for details).

3. **Glue Crawler**:
    * The Glue Crawler is triggered to automatically discover schema changes after each ETL job and update the Athena tables.

4. **Athena Workgroup**:
    * A workgroup for managing and executing queries.
    * Includes the ability to run pre-saved queries and write results to S3.

5. **Shared S3 Bucket**:
    * A single bucket used for both ETL output and Athena query results.
    * The bucket is configured to handle different environments.

---

## Scripts

There are numerous js and py scripts used in the building and deployment of the app system. Here is a quick overview:

* **JS Scripts**:
  * [build-etl](/scripts/build-etl.js): Fetches the ETL dependencies from the Python configuration project, grabs the ETL package name (which changes per version), and triggers the ETL package building process (building the wheel). This is necessary for the deployment process.
  * [config](/scripts/config.js): Configures the system using `configuration.json` and makes configurations available throughout the app. Many modules, including the ETL app (via `etl_config`), use this configuration. The CDK deploys using the configuration in `config.deploymentConfig` for the given environment which is determined based on the git branch if available. If not, it defaults to a default `dev` environment. Note that if you change your branch, you need to update the configuration to reflect the new environment.
  * [deploy](/scripts/deploy.js): Uses the configuration to deploy the CDK app (can be accessed via `npm run deploy`).
  * [run-glue-workflow](/scripts/run-glue-workflow.js): Triggers the Glue Workflow from the IDE (can be accessed via `npm run workflow`).

* **PY Scripts**:
  * [setup](/etl/setup.py): Used by the wheel to package and distribute the ETL code for Glue.
  * [build_wheel](/etl/build_wheel.py): Builds the wheel for the Poetry package for easy installation in the Glue environment.
  * [glue_job](/etl/glue-entry/glue_job.py): Runs the Glue job within the Glue environment. This, along with the runner/test, is the primary mechanism to trigger the Glue job. These scripts manage environment-specific configurations and abstract away deployment complexity.
  * [runner](/etl/runner.py): Runs the test using [test_etl_integration](/etl/tests/test_etl_integration.py) and allows you to test the system locally.

* **NPM Scripts**
  * `npm run deploy`: Deploys the CDK app by calling the deploy script.
  * `npm run workflow`: Runs the Glue Workflow by calling the run-glue-workflow script.

* **Poetry Scripts**
  * `poetry run test`: Runs the ETL test using the runner script
  * `poetry run wheel`: Builds the etl whl using the build_wheel script

---

## Analytic Store (Outputs Bucket)

* **etl_output**: Stores the output data and metadata from the ETL processes. The data is partitioned by state and upgrade. If you implement the date partitioning, it will be partitioned by date as well
* **scripts**: Houses the Glue job package and runner script (`oedi_etl`, `glue_entry.py`).

---

## Logging

For local ETL runs, there is a local logger, and for AWS Glue runs, logging is managed using `print` statements that are captured in CloudWatch. Each log file is tagged with a timestamp and the log filename. At different log levels, you can track the progress of the ETL job, with a summary provided at the end of each log.

When running on AWS Glue, logs are sent directly to CloudWatch, and only `INFO` level and higher messages are logged. This ensures that logging remains efficient and avoids excessive verbosity. For local runs, all log levels, including `DEBUG`, are available for more detailed tracking.

While the logging in AWS Glue is restricted to `INFO` level, local logging is more verbose and can help diagnose systemic issues. If you find the Glue logs too verbose, you can adjust some `logger.info` outputs to `logger.debug` to reduce the output.

Below is a sample summary printout from an actual test run. As you can see, `5790-1.parquet` was tracked as incoming but not uploaded. The `total_files_transferred_to_uploader` is 1 less than the `total_files_fetched`, which is captured by the `discrepancy_count`, and the file itself is logged in `discrepancies`. Such discrepancies can be investigated or manually addressed. In this case, it appears that the file may be corrupted and causing an error during transformation, which can be confirmed by reviewing the logs. The purpose here is not to troubleshoot, but to guide you that ETL processing failures can be identified and resolved.

```JSON
2025-03-19T20:25:12.349503 [info     ] {
    "time_stat": {
        "total_time_seconds": 564.64
    },
    "data_files_stats": {
        "total_data_files_listed": 1126,
        "total_data_files_uploaded": 1126,
        "missing_data_fetches_count": 0,
        "missing_data_transforms_count": 0,
        "missing_data_uploads_count": 0,
        "missing_data_fetches_files": {},
        "missing_data_transforms_files": {},
        "missing_data_uploads_files": {}
    },
    "metadata_files_stats": {
        "total_metadata_files_listed": 2,
        "total_metadata_files_uploaded": 2,
        "missing_metadata_fetches_count": 0,
        "missing_metadata_bypasses_count": 0,
        "missing_metadata_uploads_count": 0,
        "missing_metadata_fetches_files": {},
        "missing_metadata_bypasses_files": {},
        "missing_metadata_uploads_files": {}
    }
}
```

---

## Known Issues/Limitations

Use the TODO extension to browse through the list of TODOs if you decide to maintain this.

* **Crawler Metadata Handling**: The automated crawlers map all metadata to the same parquet-prefixed table regardless of state. While the ETL is state-specific, all metadata across states will share the same schema, which could cause errors in some queries if the schema differs by state. Handling custom crawlers for this is likely not worth the effort. Note that the last job always hogs this schema so if you restrict your querying to the last job, you should be fine.

* **State-Specific Job and Prefixing**: Each job and data crawling is performed on a per-state basis, with schema table prefixed for each state as creating multiple tables per state doesn’t make sense nor crawling all states (wasteful). The downside is the need to deal with state suffixes in SQL queries.

* **Untested Parallel State Jobs**: This system should handle multiple jobs with the other restrictions in mind. However, it has not been tested with multiple state jobs running in parallel. You can test or restrict your ETL to one state ETL job per one workflow run.

* **Glue Logging Restrictions**: Logging on Glue is limited to CloudWatch. While it's theoretically possible to use a custom logger to log to S3, this would require handling Glue intricacies, as Glue utilities monopolize stdout. To save on cost and clutter, DEBUG is not supported on Glue.

* **Single Log File**: Currently, all logging is written to a single log file. This could be improved by writing to separate log files for each ETL job run. Although different ETL runs have unique timestamps, running multiple jobs per state will share the same log file.

* **ETL Optimization**: The ETL process can be further optimized, though it already has a decent utilization rate. This is not worth the effort unless you dealing with high volume + rate data.

---
