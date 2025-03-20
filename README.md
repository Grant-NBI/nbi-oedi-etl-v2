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

---

### **Versions and Metadata Handling**

This system supports two metadata versions:

* **Version `1.0` (Legacy)**
  * Uses **state-level** metadata exclusively.
  * Metadata is stored under `by_state/state={state}/parquet`.
  * Metadata filenames follow the format:

    ```
    {state}_{upgrade_str}_metadata_and_annual_results.parquet
    ```

* **Version `2.0` (Current System)**
  * Supports **both** state and county-level metadata.
  * **State-Level Metadata (No `counties` key present)**
    * Tracks the `by_state` folder.
    * Aggregated at the **state level**, not at the county level.
    * Metadata filenames follow the format:

      ```
      metadata_and_annual_results_aggregates/by_state/full/parquet/state={state}/{state}_{upgrade_str}_agg.parquet
      ```

  * **County-Level Metadata (Explicit `counties` key present)**
    * Tracks the `by_state_and_county` folder.
    * Supports **explicit county selection** and **fetching all counties dynamically**.
    * Metadata filenames follow the format:

      ```
      metadata_and_annual_results_aggregates/by_state_and_county/full/parquet/state={state}/county={county}/{state}_{county}_{upgrade_str}_agg.parquet
      ```

  * **Wildcard County Fetch (`counties: ["*"]`)**
    * Dynamically retrieves all available counties from the S3 structure.
    * Uses `by_state_and_county` metadata paths.

---

### **Output Configuration**

* The ETL output is written to a configurable S3 directory under the staging/analytics bucket.
* The directory structure is environment-based, ensuring that different environments (e.g., dev, prod) are kept separate.
* All ETL output is written to a single table, ensuring that the querying process is streamlined. The table is automatically updated by the Glue Crawler after each ETL run.

```json
{
  "monorepoRoot": ".",
  "deploymentConfig": [
    {
      "appName": "NbiBuildingAnalytics",
      "account": "xxx",
      "deploymentEnv": "dev",
      "profile": "profile_name",
      "regions": [
        "us-west-2"
      ],
      "glueJobTimeoutMinutes": 240,
      "requireApproval": "never"
    }
  ],

  "etl_config": {
    "output_dir": "etl_output",
    "src_bucket": "oedi-data-lake",

    "data_partition_in_release": "timeseries_individual_buildings/by_state",
    "base_partition": "oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock",

    "job_specific": [
      {
        // Legacy mode (Version 1.0) - State-Level Only
        "metadata_root_dir": "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_1/metadata_and_annual_results/by_state/parquet",
        "relative_metadata_prefix_type": "1.0",
        "release_name": "comstock_amy2018_release_1",
        "release_year": "2024",
        "state": "CA",
        "upgrades": ["0", "1"]
      },
      {
        // Version 2.0 - State-Level Aggregated (Absence of "counties")
        // **Tracks by_state folder**
        "metadata_root_dir": "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results",
        "relative_metadata_prefix_type": "2.0",
        "release_name": "comstock_amy2018_release_2",
        "release_year": "2024",
        "state": "TX",
        "upgrades": ["0", "1"]
      },
      {
        // Version 2.0 - County-Level with Explicit Counties
        // **Tracks by_state_and_county folder**
        "metadata_root_dir": "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results",
        "relative_metadata_prefix_type": "2.0",
        "release_name": "comstock_amy2018_release_2",
        "release_year": "2024",
        "state": "TX",
        "counties": ["G4802010", "G4802590", "G4802700"],
        "upgrades": ["0", "1"]
      },
      {
        // Version 2.0 - Fetching All Counties
        // **Tracks by_state_and_county folder**
        "metadata_root_dir": "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_2/metadata_and_annual_results",
        "relative_metadata_prefix_type": "2.0",
        "release_name": "comstock_amy2018_release_2",
        "release_year": "2024",
        "state": "NY",
        "counties": ["*"], // Fetches all counties dynamically
        "upgrades": ["0", "1"]
      }
    ],

    "settings": {
      "log_dir": "logs",
      "log_filename": "etl.log",
      "logging_level": "INFO",
      "idle_timeout_in_minutes": 5,
      "listing_page_size": 500,
      "max_listing_queue_size": 1000
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
   * The metadata, located in the `metadata_and_annual_results` directory, provides building-specific information.
   * Metadata is stored in two formats depending on the version:
     * **Version 1.0 (Legacy, State-Level Only)**:
       * Metadata is partitioned under `by_state` and follows the format:

       ```md
       CA_baseline_metadata_and_annual_results.parquet
       CA_upgrade01_metadata_and_annual_results.parquet
       CA_upgrade02_metadata_and_annual_results.parquet
       ```

     * **Version 2.0 (Current, State and County Level)**:
       * **State-Level Metadata (No `counties` key present)**
         * Metadata is stored in `by_state/full/parquet/`
         * Example:

         ```md
         state=CA/CA_baseline_agg.parquet
         state=CA/CA_upgrade01_agg.parquet
         ```

       * **County-Level Metadata (Explicit `counties` key present)**
         * Metadata is stored in `by_state_and_county/full/parquet/`
         * Example:

         ```md
         state=CA/county=G0600010/CA_G0600010_baseline_agg.parquet
         state=CA/county=G0600020/CA_G0600020_upgrade01_agg.parquet
         ```

       * **Wildcard County Fetch (`counties: ["*"]`)**
         * All counties are dynamically retrieved and processed under `by_state_and_county`.

   * The ETL automatically determines the correct metadata format and paths based on the version and configuration.

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

This is the core component of the system and is a python poetry package distributed as a whl.

#### Key Operations in ETL

1. **Extraction**:
    * The ETL begins by reading from the S3 bucket using filters based on parameters such as release year and state.
    * Metadata is stored alongside the main table in the output directory. The is directly copied from the source to destination without any fetching as it does not need transformation. Joining the metadata during this phase may help for the project specific query performance, however it comes at huge storage cost and is avoided.In scenario where the joining can be found effective, this bypass should be modified. See [fetch](/etl/oedi_etl/fetch.py)

2. **Transformation**:
    * Data is aggregated from 15-minute intervals to 1-hour intervals.
    * The ETL has a configurable flag to enable/disable date partitioning, improving performance for time-series queries.
    * Other transformations such as filtering for only specified columns, rounding floating points, partitioning by date, etc. can be performed at this phase. See [transformation](/etl/oedi_etl/transform.py)
    * NOTE:originally, a toggle partition into date was used but removed for simplicity. This transformation can be highly effective for intensive time series analysis.

3. **Load**:
    * Transformed data is written to an S3 bucket shared with Athena.
    * Output is preserved in Parquet format and compressed using Snappy for speed and performance.
    * See [upload](/etl/oedi_etl/upload.py)

#### Supporting systems

1. *[etl_job](/etl/oedi_etl/etl_job.py)*. Orchestrate the ETL (fetch, transform and upload) tasks along with the monitoring per every job.
2. *[main](/etl/oedi_etl/main.py)*. Orchestrate multiple etl_jobs as well as update the latest partitions as s3 targets for the crawlers.
3. *[log](/etl/oedi_etl/log.py)*. Allows logging across multiple workers (processes spread across CPUS) as well as async tasks.
4. *[monitor](/etl/oedi_etl/monitor.py)*. Track incoming files, processed files, uploaded files, transformations, and discrepancies and logs summary of the ETL job. It also monitor tasks and handle idle timeout.
"""

#### Key Design Considerations

Once Glue resources are provisioned, costs are incurred by the second, and the design focuses on balancing cost and performance as follows:

* **Async I/O** for S3 file listing, fetching, and uploading, ensuring these operations do not block other tasks.
* **Separate worker pools** handle input (fetching) and output (uploading) I/O tasks independently for better parallelization.
* **Multiprocessing for CPU-bound tasks** such as data transformation and aggregation to fully utilize available CPU cores.
* **Queue-based task management** to decouple I/O and processing operations, allowing for efficient task distribution across workers.
* **Parallel partition handling** where different state-upgrade combinations are processed concurrently for faster overall performance.

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
{
    "job_name": "comstock_amy2018_release_1_2024_AK",
    "start_time": "2024-10-01 07:50:07",
    "total_time_seconds": 180.03,
    "end_time": "2024-10-01 07:53:07",
    "total_files_listed": 504,
    "total_files_fetched": 504,
    "total_files_bypassed": 2,
    "total_files_transferred_to_worker": 504,
    "total_transformed_files": 502,
    "total_files_transferred_to_uploader": 503,
    "total_uploaded_files": 503,
    "discrepancies_count": 1,
    "discrepancies": [
        {
            "stage": "Listed but not Uploaded",
            "files": [
                "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_1/timeseries_individual_buildings/by_state/upgrade=1/state=AK/5790-1.parquet"
            ]
        }
    ]
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
