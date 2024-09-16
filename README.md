# Airflow DAG Creation Function: `create_dag`

## Overview
The `create_dag` function dynamically creates an Airflow DAG based on the provided parameters, allowing for flexible scheduling and configuration options. The function includes conditional logic to handle different scheduling needs and is designed to manage a variety of parameters related to DAG execution, service accounts, and tagging.

## Purpose
- Creates and configures a DAG for Airflow based on inputs such as schedule, service accounts, and default arguments.
- Supports conditional scheduling depending on whether the user provides a specific schedule or relies on a dataset-driven schedule.

## Parameters
- **dag_id**: Unique identifier for the DAG.
- **max_active_runs**: Maximum number of active runs allowed at the same time for the DAG.
- **catchup**: A Boolean to enable or disable the execution of past DAG runs that are behind the current schedule.
- **schedule**: Defines the schedule interval for the DAG. If set to `'No schedule'`, the function uses `dataset_schedule`.
- **default_args**: Dictionary containing default parameters for tasks within the DAG.
- **sublocation**: Likely refers to a specific location or environment for the DAG (may need further context).
- **master_batch_payload_limit**: Payload limit for handling batch jobs in the DAG.
- **subarr**: Likely an array containing task-related data (may need further clarification).
- **master_file**: The primary file to be processed by the DAG.
- **service_account**: The Google Cloud service account used for task execution.
- **label_key**: Key used for tagging the DAG or associated jobs.
- **pipeline_name**: Name of the pipeline to which this DAG belongs.
- **audit_details**: Details related to logging and auditing of DAG runs.
- **create_cross_cluster_trigger**: Flag indicating whether cross-cluster triggers should be set up.
- **dataset_schedule**: Custom schedule interval if no `schedule` is provided.
- **dataset_schedule_trigger**: Trigger setting related to dataset scheduling.
- **desc**: Description of the DAG.
- **tag_array**: Tags associated with the DAG for categorization in Airflow.

## Returns
- **DAG object**: Returns a configured DAG object that can be used in Airflow's task scheduling system.

## Authentication
- The function expects a service account to be passed as a parameter (`service_account`) for executing tasks that require Google Cloud authentication.

## Functionality
1. **Conditional Scheduling**:
   - If `schedule` is `'No schedule'`, the function assigns `dataset_schedule` as the DAG’s schedule.
   - Otherwise, the provided `schedule` is used.
   
2. **DAG Creation**:
   - A DAG object is instantiated using the provided parameters, including the schedule interval, description, default arguments, maximum active runs, catchup settings, and tags.
   
3. **Initialization**:
   - The `sensor_count` is initialized to zero, potentially as a placeholder for future logic related to sensors.

---
# With DAG:
## Dataflow Job Launch Function: `_start_template_dataflow`

## Overview
The `_start_template_dataflow` function launches a Google Cloud Dataflow job using a pre-configured template stored in Google Cloud Storage. It dynamically builds the runtime environment based on input variables, submits the job request to the Dataflow API, and monitors the job until it completes.

## Purpose
- Initiates a Dataflow job using a provided GCS path template, along with job-specific parameters and runtime configurations.
- Monitors and waits for the Dataflow job to complete, ensuring that the job runs successfully.

## Parameters
- **name**: The name of the Dataflow job.
- **variables**: A dictionary containing configuration settings, including the project ID, region, worker settings, and other environment-specific options.
- **parameters**: A dictionary of parameters to be passed to the Dataflow template for the job execution.
- **dataflow_template**: The Google Cloud Storage path where the Dataflow template is stored (e.g., `gs://path/to/template`).

## Returns
- **response**: The API response returned after the Dataflow job launch, containing details such as the job ID and status information.

## Authentication
- The function authenticates via the Google Cloud connection retrieved from `self.get_conn()`, which uses appropriate credentials to interact with the Dataflow API.

## Functionality
1. **Runtime Environment Construction**:
   - The function loops through specific keys in the `variables` dictionary to build the `environment` dictionary. This dictionary is used to configure the Dataflow job's runtime environment, such as the number of workers, zone, service account, and machine type.
   
2. **Dataflow Job Submission**:
   - Using the Dataflow API, the function constructs a request to submit the job. The request includes the project ID, region, GCS path to the Dataflow template, job parameters, and the runtime environment.
   
3. **Job Monitoring**:
   - After submitting the job, the function monitors its status through the `_DataflowJobsController`, which polls the job’s progress at regular intervals, waiting for it to complete or fail.
   
4. **Variables Update**:
   - The `variables` dictionary is updated using the `_set_variables()` function, which ensures the variables reflect any new context or configuration needed for the job.

5. **Response Handling**:
   - Once the job is successfully launched, the API response is returned, providing details of the submitted Dataflow job.

---

# Cross-Cluster Trigger Creation

## Overview
This section of the code handles the creation of a cross-cluster trigger within an Airflow DAG. It uploads a file to a Google Cloud Storage (GCS) bucket that acts as a signal for downstream processes in a different cluster. The trigger is dependent on the Airflow DAG's schedule timestamp.

## Purpose
- Creates a cross-cluster trigger that signals downstream tasks in different clusters.
- Uploads a trigger file to a designated Google Cloud Storage bucket, which contains a timestamp to coordinate execution between clusters.

## Parameters
- **create_cross_cluster_trigger**: A boolean flag indicating whether the cross-cluster trigger functionality is enabled.
- **file_nm**: The name of the file to be uploaded as a trigger. Defaults to the DAG ID.
- **dag_id**: The unique identifier for the DAG, used to name the file if no specific file name is provided.
- **context**: Airflow's execution context, which includes information such as the DAG's schedule timestamp (`ts`).

### Internal Variables:
- **COMPOSER_DEPENDANCY_BUCKET**: The name of the Google Cloud Storage bucket where the trigger file is uploaded. Derived from an environment variable.

## Returns
- **create_cross_cluster_trigger**: A dynamically created `PythonOperator` that uploads the cross-cluster trigger to GCS.

## Authentication
- **Google Cloud Storage**: The code uses the `google.cloud.storage.Client()` to authenticate and interact with a Composer dependency bucket for uploading the cross-cluster trigger file.

## Functionality

1. **Check if Cross-Cluster Trigger is Enabled**:
   - The function checks if the `create_cross_cluster_trigger` flag is set to `True`. If so, it proceeds to define a custom Python function for uploading a trigger file.

2. **Trigger Creation Function (`create_cross_cluster_trigger_fun`)**:
   - This internal function constructs the trigger file name using the DAG's schedule timestamp (`ts`). The timestamp is split into date and hour components to create a formatted string (`schedule_dts`), which is then appended to the `file_nm` to generate a unique file name.
   - The function then retrieves the bucket name from an environment variable and uploads the trigger file to the specified bucket in Google Cloud Storage.

3. **PythonOperator for Cross-Cluster Trigger**:
   - A `PythonOperator` is dynamically created to call the `create_cross_cluster_trigger_fun` function. The operator is linked to the `end` task of the DAG, ensuring that the trigger is uploaded after all other tasks have completed.

4. **Task Dependencies**:
   - The `end` task is configured to trigger the `create_cross_cluster_trigger` task if cross-cluster triggering is enabled.

# CIF Trigger Function

## Overview
This function handles the triggering of CIF (Customer Information File) events in an Airflow DAG. It uploads a specific file to a Google Cloud Storage (GCS) bucket, based on the configuration provided in the DAG parameters. If the CIF trigger is not present in the parameters, a `DummyOperator` is used as a placeholder.

## Purpose
- Executes a file upload to a Google Cloud Storage bucket that triggers downstream tasks for a specific CIF event.
- Uses impersonated credentials for secure access to GCS.

## Parameters
- **parameter['cif_trigger']**: A dictionary containing the necessary information for the CIF trigger, including `db_type` and `file_name`.
- **file_nm**: A dictionary passed to the CIF trigger function containing details about the database type and file name.
- **table_name**: The name of the table, dynamically used to create task IDs.
- **context**: The Airflow execution context, passed through the `PythonOperator`.

### Internal Variables:
- **IMPERSONATION_CHAIN**: The service account used to impersonate credentials for accessing GCS. Derived from the Airflow environment variable `AIRFLOW_VAR_WORK_PROJECT`.
- **logs_bucket**: The name of the GCS bucket where logs are stored, built using the environment variable `AIRFLOW_VAR_ENV`.

## Returns
- **start_<table_name>**: A dynamically created `PythonOperator` or `DummyOperator`, depending on whether the CIF trigger is present in the parameters.

## Authentication
- **Impersonated Credentials**: The function uses `google.auth.impersonated_credentials.Credentials` to impersonate the target service account for secure access to the GCS bucket.
- **Google Cloud Storage**: The `storage.Client` is initialized with the impersonated credentials to interact with GCS.

## Functionality

1. **Check for CIF Trigger in Parameters**:
   - The function checks if the `cif_trigger` is present in the parameters. If so, it proceeds to define a custom Python function (`cif1_trigger`) to upload the trigger file to GCS.

2. **CIF Trigger Function (`cif1_trigger`)**:
   - This function generates the trigger file name using the `db_type` and `file_name` from the `file_nm` dictionary. It then uploads the file to the specified GCS bucket.

3. **Impersonated Credentials**:
   - The function uses impersonated credentials to securely access the GCS bucket. If no impersonated credentials are available, default credentials are used.

4. **PythonOperator or DummyOperator**:
   - A `PythonOperator` is dynamically created to call the `cif1_trigger` function. If the `cif_trigger` is not present in the parameters, a `DummyOperator` is used instead.

5. **Task Dependencies**:
   - The dynamically created operator is assigned to the `start_<table_name>` and `preprocess_<table_name>` tasks to control the workflow.

# GCS File Dependence Sensor Setup

## Overview
This code section handles the setup of Google Cloud Storage (GCS) sensors in an Airflow DAG. These sensors check for the existence of specific files in GCS buckets, using a predefined prefix and bucket structure. If the file dependence condition is `True`, a sensor is dynamically created for each bucket and prefix combination.

## Purpose
- Dynamically create GCS sensors to check for the existence of files in GCS based on the DAG's schedule.
- Ensure that specific files exist in GCS buckets before triggering downstream tasks in the DAG.

## Parameters
- **parameter['file_dependance']**: A boolean flag that indicates whether file dependence is required for the DAG.
- **parameter['sensor_bucket']**: A list of GCS bucket names, where each bucket is formatted using the environment variable `AIRFLOW_VAR_ENV`.
- **parameter['sensor_file_prefix']**: A list of file prefixes to be used by the GCS sensors when searching for files.
- **parameter['sensor_timeout']**: A list of timeouts for the GCS sensors, determining how long they will wait for the specified file to appear.
- **table_name_full**: The full name of the table, used to create task IDs dynamically.
- **schedule_dts**: The schedule timestamp used to construct the file prefix for the GCS sensor.

### Internal Variables:
- **IMPERSONATION_CHAIN**: The service account used to impersonate credentials for accessing GCS. Derived from the Airflow environment variable `AIRFLOW_VAR_WORK_PROJECT`.

## Returns
- **GCSObjectsWithPrefixExistenceSensor**: A dynamically created GCS sensor for each file prefix and bucket combination.
- **Task ID (tid)**: The task ID is dynamically generated and truncated if necessary to comply with the character limit (max 175 characters for `tid` + `dag_id`).

## Authentication
- **Impersonated Credentials**: The function uses the `IMPERSONATION_CHAIN` to authenticate and access GCS resources.
- **Google Cloud Storage**: The sensor uses the default Google Cloud connection ID (`google_cloud_default`) to interact with GCS.

## Functionality

1. **Check for File Dependence**:
   - The function first checks if `parameter['file_dependance']` is set to `True`. If so, it proceeds to create GCS sensors for the specified buckets and prefixes.

2. **Dynamically Create Sensors**:
   - For each bucket and file prefix combination, a new `GCSObjectsWithPrefixExistenceSensor` is created. The bucket name is formatted using the environment variable `AIRFLOW_VAR_ENV`, and the file prefix is combined with the schedule timestamp (`schedule_dts`).
   - A task ID (`tid`) is generated dynamically. If the length of the `tid` combined with the `dag_id` exceeds 175 characters, the task ID is truncated and shortened to ensure it complies with Airflow's character limit.

3. **Task Dependencies**:
   - The `start_<table_name>` task triggers the dynamically created sensors, which in turn trigger the `preprocess_<table_name>` task once the files are detected in GCS.
   
4. **Sensor Configuration**:
   - Each sensor uses the `IMPERSONATION_CHAIN` for authentication and has a specific timeout configured via `parameter['sensor_timeout'][sensor]`. Sensors are pooled using the `pool_sensor` resource.
  
# Cross-Cluster Dependence Setup

## Overview
This code handles the creation of sensors for cross-cluster file dependencies in an Airflow DAG. The sensors check if specific dependency files exist in a Google Cloud Storage (GCS) bucket, based on the scheduled timestamp and DAG configuration.

## Purpose
- Dynamically create GCS sensors to check for cross-cluster file dependencies based on the DAG schedule and file existence in GCS.
- Handle cross-cluster data dependencies by ensuring certain files are available before triggering downstream tasks.

## Parameters
- **parameter['cross_cluster_dependance']**: A boolean flag that indicates whether cross-cluster dependence is required for the DAG.
- **parameter['cross_dependancy_dag_id']**: A list of DAG IDs that represent the cross-cluster dependencies.
- **parameter['cross_dependancy_execution_delta']**: A list of time delta expressions used to calculate the execution time for the dependency check.
- **parameter['cross_dependancy_day']**: A list indicating whether the dependency check is based on daily or hourly intervals.
- **parameter['cross_dependancy_sensor_timeout']**: A list of timeouts for the cross-cluster sensors, determining how long they will wait for the specified files to appear.

### Internal Variables:
- **IMPERSONATION_CHAIN**: The service account used to impersonate credentials for accessing GCS. Derived from the Airflow environment variable `AIRFLOW_VAR_WORK_PROJECT`.
- **bucket_tre**: The GCS bucket where cross-cluster dependencies are stored, constructed using the environment variable `AIRFLOW_VAR_ENV`.

## Returns
- **GCSObjectsWithPrefixExistenceSensor**: A dynamically created GCS sensor for each cross-cluster dependency file.
- **Task ID (tid)**: The task ID is dynamically generated and truncated if necessary to comply with the character limit (max 175 characters for `tid` + `dag_id`).

## Authentication
- **Impersonated Credentials**: The function uses the `IMPERSONATION_CHAIN` to authenticate and access GCS resources.
- **Google Cloud Storage**: The sensor uses the default Google Cloud connection ID (`google_cloud_default`) to interact with GCS.

## Functionality

1. **Check for Cross-Cluster Dependence**:
   - The function checks if `parameter['cross_cluster_dependance']` is set to `True`. If so, it proceeds to create GCS sensors for the cross-cluster dependencies specified in the DAG parameters.

2. **Dynamically Create Sensors**:
   - For each dependency listed in `parameter['cross_dependancy_dag_id']`, the function generates a schedule timestamp (`schedule_dts_cross_cluster`) based on whether the dependency is daily or hourly.
   - A task ID (`tid`) is generated dynamically. If the length of the `tid` combined with the `dag_id` exceeds 175 characters, the task ID is truncated and shortened to ensure it complies with Airflow's character limit.

3. **Task Dependencies**:
   - The `start_<table_name>` task triggers the dynamically created sensors, which in turn trigger the `preprocess_<table_name>` task once the dependency files are detected in GCS.

4. **Sensor Configuration**:
   - Each sensor uses the `IMPERSONATION_CHAIN` for authentication and has a specific timeout configured via `parameter['cross_dependancy_sensor_timeout'][sensor]`. Sensors are pooled using the `pool_sensor` resource.

# DAG Dependence Setup

## Overview
This section of the code creates an Airflow `ExternalTaskSensor` for monitoring the completion of external DAG tasks before proceeding with downstream tasks in the current DAG. The external dependencies are dynamically created based on the configuration passed in the `parameter` dictionary.

## Purpose
- Create sensors to monitor external DAG task completions.
- Ensure that the current DAG's tasks only execute after the external tasks have successfully completed.

## Parameters
- **parameter['dag_dependance']**: A boolean flag indicating whether external DAG task dependencies should be set up.
- **parameter['dag_external_dag_id']**: A list of external DAG IDs that the current DAG depends on.
- **parameter['dag_external_task_id']**: A list of external task IDs within the specified DAGs that need to be monitored.
- **parameter['execution_delta']**: A list of time deltas used to adjust the external task execution times relative to the current DAG schedule.
- **parameter['ext_sensor_timeout']**: A list of timeouts for each `ExternalTaskSensor`, determining how long the sensor will wait for the external tasks to complete.

### Internal Variables:
- **tid**: The task ID for each sensor, dynamically generated based on the external DAG and task IDs. If the total length of the `tid` and `dag_id` exceeds 175 characters, the task ID is shortened.
- **sensor_count**: A counter used to generate unique task IDs if they exceed the character limit.

## Returns
- **ExternalTaskSensor**: A dynamically created `ExternalTaskSensor` for monitoring external DAG task dependencies.
- **Task ID (tid)**: A unique task ID for each sensor. If the `tid` exceeds the character limit, it is truncated to ensure it complies with Airflow's constraints.

## Authentication
- No additional authentication is required as the `ExternalTaskSensor` uses Airflow's internal mechanisms to monitor the external DAGs.

## Functionality

1. **Check for DAG Dependence**:
   - The function checks if `parameter['dag_dependance']` is set to `True`. If so, it proceeds to create external task sensors for each dependency specified in the parameters.

2. **Dynamically Create Sensors**:
   - For each external DAG ID and task ID in `parameter['dag_external_dag_id']` and `parameter['dag_external_task_id']`, a corresponding `ExternalTaskSensor` is created.
   - The task ID (`tid`) is generated based on the external DAG and task IDs. If the combined length of the `tid` and `dag_id` exceeds 175 characters, the task ID is shortened.

3. **Task Dependencies**:
   - The `start_<table_name>` task triggers the dynamically created external task sensors. Once the external task is confirmed to have completed, the sensor triggers the `preprocess_<table_name>` task.

4. **Sensor Configuration**:
   - Each sensor monitors a specific external DAG task using `external_dag_id` and `external_task_id`.
   - The `execution_delta` is used to adjust the external task execution time relative to the current DAG's schedule.
   - Each sensor has a timeout, set by `parameter['ext_sensor_timeout'][ext_sensor]`, to control how long it will wait for the external task to complete. Sensors are pooled using the `pool_sensor` resource.
  
   
# UUID Generation for File Processing

## Overview
This part of the pipeline dynamically generates a UUID based on the file destination folder and whether unique processing is enabled. It utilizes the Airflow `PythonOperator` to generate this UUID, which is subsequently used to ensure unique file processing in the pipeline.

## Purpose
- Generate a unique identifier (UUID) for file processing if `processing_unique` is enabled.
- Append this UUID to the destination folder path to create a unique folder name for processing.
- Trigger the next steps in the DAG once the UUID is generated.

## Parameters

- **move_files_dest_folder**: The destination folder where files will be moved after processing.
- **parameter['processing_unique']**: A boolean flag indicating whether a unique identifier (UUID) should be appended to the folder name for each processing cycle.
- **adjust_timezone_hr**: An integer specifying the number of hours to adjust from UTC when generating the unique identifier timestamp.

### Internal Variables:
- **table_name**: The name of the table being processed.
- **task_id**: Dynamically generated task IDs for the UUID generation and processing tasks.

## Returns
- **PythonOperator**: A dynamically created `PythonOperator` responsible for executing the `generate_uuid_from_name` function.
- **UUID**: If `processing_unique` is `True`, a UUID is generated and appended to the folder name along with a timestamp.

## Authentication
- No additional authentication is required, as this operation is internal to the DAG and does not interact with external services.

## Functionality

1. **UUID Generation**:
   - The function `generate_uuid_from_name` generates a unique identifier based on the `move_files_dest_folder` and the current UTC time. This UUID is appended to the folder name if `processing_unique` is set to `True`.
   - If `processing_unique` is `False`, the function returns the folder name as-is without generating a UUID.

2. **Task Creation**:
   - The DAG dynamically creates a `PythonOperator` task to execute the `generate_uuid_from_name` function for each table. The task is named `generate_uuid_<table_name>` and is inserted into the DAG.

3. **Task Dependencies**:
   - The `generate_uuid_<table_name>` task is executed after the `preprocess_<table_name>` task.
   - Once the UUID is generated (or the folder name is retrieved), the pipeline proceeds to the `start_processing_<table_name>` task.

4. **Conditional UUID Generation**:
   - If `processing_unique` is `True`, the DAG ensures a unique folder is created for each processing run.
   - If `processing_unique` is `False`, the same folder is reused across multiple DAG runs.
  
# Table Count Check Function

## Overview

The `table_count_check` function performs a row count validation on a BigQuery table. It ensures that the number of rows in the specified table falls within a predefined acceptable range. This function utilizes Google Cloud's BigQuery client and impersonated credentials for secure access.

## Purpose

- Validate that a BigQuery table contains a number of rows within the specified `min_count` and `max_count`.
- Ensure data integrity by checking that the table is not unexpectedly empty or excessively large.
- Raise an exception if the row count is outside the defined valid range, preventing downstream processes from proceeding with invalid data.

## Parameters

- **work_table**: *(str)*  
  The name of the BigQuery table to check.

- **gcp_project**: *(str)*  
  The Google Cloud project ID where the BigQuery table resides.

- **airflow_conn_id**: *(str)*  
  The Airflow connection ID for retrieving credentials (currently not used; commented out in the code).

- **wrk_project**: *(str)*  
  The working project name (may be used for logging or additional context).

- **tmp_table_project**: *(str)*  
  The project ID where the temporary BigQuery table is located.

- **min_count**: *(int)*  
  The minimum acceptable number of rows in the table.

- **max_count**: *(int)*  
  The maximum acceptable number of rows in the table.

- **IMPERSONATION_CHAIN**: *(str)*  
  The service account email used for impersonation to authenticate BigQuery access.

## Returns

- **None**:  
  The function does not return any value but will raise an exception if the row count is outside the valid range.

## Authentication

- **Impersonated Credentials**:  
  Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to BigQuery resources without relying on default credentials.

## Functionality

1. **Impersonation Setup**:
   - Obtains the default credentials using `google.auth.default()`.
   - Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the necessary scope `https://www.googleapis.com/auth/cloud-platform`.

2. **BigQuery Client Initialization**:
   - Initializes a BigQuery client using the impersonated credentials and the specified `gcp_project`.

3. **Construct and Execute Query**:
   - Constructs a SQL query to count the number of rows in the specified table:
     ```sql
     SELECT COUNT(*) AS cnt FROM `<tmp_table_project>.<work_table>`
     ```
   - Executes the query using the BigQuery client.
   - Retrieves the result and extracts the row count.

4. **Row Count Validation**:
   - Converts the row count to an integer.
   - Checks if the row count is less than or equal to `min_count` or greater than or equal to `max_count`.
   - If the count is outside the valid range, raises an exception with the message:
     ```
     "Table count check is outside the defined valid range"
     ```

5. **Error Handling**:
   - Wraps the execution in a `try-except` block.
   - In case of any exceptions (e.g., query failure, authentication issues), it prints the exception and re-raises it to halt execution.

6. **Function Invocation**:
   - The function `table_count_check` is called with the provided parameters to perform the validation.

---

**Note**: This function is crucial for maintaining data quality and integrity within data pipelines that rely on accurate and expected data volumes in BigQuery tables.

# venv_bq_load_data_task

## Overview

The `venv_bq_load_data_task` function is designed to load data into a temporary BigQuery table within a virtual environment. It leverages impersonated credentials for secure access and utilizes an external package `load_temp_table` to perform the actual data loading process.

## Purpose

- Load data into a temporary BigQuery table using specified parameters.
- Utilize impersonated credentials for secure authentication.
- Serve as a wrapper around the `load_temp_table.load_table` function, providing necessary credentials and parameters.

## Parameters

- **work_table**: *(str)*  
  The name of the temporary BigQuery table where data will be loaded.

- **job_id**: *(str)*  
  A unique identifier for the load job.

- **file_format**: *(str)*  
  The format of the source data files (e.g., 'CSV', 'JSON', 'Avro').

- **use_logical_type_flag**: *(bool)*  
  A flag indicating whether to use logical types when loading data.

- **gcp_project**: *(str)*  
  The Google Cloud Project ID where the BigQuery dataset resides.

- **file_path**: *(str)*  
  The path to the source data files to be loaded.

- **expiration_hours**: *(int)*  
  The number of hours after which the temporary table will expire.

- **label_key**: *(str)*  
  A key for labeling the BigQuery job.

- **label_value**: *(str)*  
  A value associated with the label key for the BigQuery job.

- **json_config**: *(dict)*  
  JSON configuration parameters for the data load.

- **airflow_conn_id**: *(str)*  
  The Airflow connection ID used to retrieve credentials (commented out in the code).

- **max_bad_records**: *(int)*  
  The maximum number of bad records allowed during the load.

- **allow_quoted_newlines**: *(bool)*  
  Whether to allow quoted newlines in CSV data.

- **quote_character**: *(str)*  
  The character used for quoting in CSV data.

- **autodetect**: *(bool)*  
  Whether to enable schema autodetection.

- **replace_linebreaker**: *(bool)*  
  Whether to replace line breakers in the data.

- **xml_line_size**: *(int)*  
  The line size to use when processing XML files.

- **field_delimiter**: *(str)*  
  The character used to separate fields in the data files.

- **skip_leading_rows**: *(int)*  
  The number of leading rows to skip in the data files.

- **skip_tail_rows**: *(int)*  
  The number of trailing rows to skip in the data files.

- **col_fix_width**: *(list)*  
  A list specifying fixed-width column sizes.

- **null_marker**: *(str)*  
  The marker used to represent null values in the data.

- **tmp_tbl_schema**: *(dict)*  
  The schema of the temporary table.

- **IMPERSONATION_CHAIN**: *(str)*  
  The service account email used for impersonation to authenticate BigQuery access.

- **file_encoding**: *(str)*  
  The encoding of the source data files.

- **auto_add_columns**: *(bool)*  
  Whether to automatically add missing columns during the load.

## Returns

- **None**:  
  The function does not return any value. It initiates the data load process into BigQuery.

## Authentication

- **Impersonated Credentials**:  
  The function uses `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to BigQuery resources without relying on default credentials.

## Functionality

1. **Set Up Impersonated Credentials**:
   - Obtains the default credentials using `google.auth.default()`.
   - Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the necessary scope `https://www.googleapis.com/auth/cloud-platform`.

2. **Load Data Using `load_temp_table`**:
   - Calls the `load_temp_table.load_table` function with all the provided parameters.
   - Passes the impersonated credentials (`tcreds`) to ensure secure access.
   - The `load_table` function handles the actual data loading into the temporary BigQuery table.

3. **Commented Out Code**:
   - Note that the code for retrieving credentials via `airflow_conn_id` is commented out, indicating that impersonated credentials are preferred or currently in use.

---

**Note**: This function is essential for data pipelines that require loading data into BigQuery while utilizing secure authentication methods. It provides flexibility in handling various data formats and configurations.

# venv_bq_load_ctl_task

## Overview

The `venv_bq_load_ctl_task` function is designed to load control data into a temporary BigQuery table within a virtual environment. It leverages impersonated credentials for secure access and utilizes an external package `load_temp_table` to perform the data loading process.

## Purpose

- Load control data into a temporary BigQuery table using specified parameters.
- Utilize impersonated credentials for secure authentication.
- Serve as a wrapper around the `load_temp_table.load_table` function, providing necessary credentials and parameters.

## Parameters

- **work_table**: *(str)*  
  The name of the temporary BigQuery table where control data will be loaded.

- **job_id**: *(str)*  
  A unique identifier for the load job.

- **file_format**: *(str)*  
  The format of the source data files (e.g., 'CSV', 'JSON', 'Avro').

- **use_logical_type_flag**: *(bool)*  
  A flag indicating whether to use logical types when loading data.

- **gcp_project**: *(str)*  
  The Google Cloud Project ID where the BigQuery dataset resides.

- **file_path**: *(str)*  
  The path to the source data files to be loaded.

- **expiration_hours**: *(int)*  
  The number of hours after which the temporary table will expire.

- **label_key**: *(str)*  
  A key for labeling the BigQuery job.

- **label_value**: *(str)*  
  A value associated with the label key for the BigQuery job.

- **json_config**: *(dict)*  
  JSON configuration parameters for the data load.

- **airflow_conn_id**: *(str)*  
  The Airflow connection ID used to retrieve credentials (commented out in the code).

- **max_bad_records**: *(int)*  
  The maximum number of bad records allowed during the load.

- **allow_quoted_newlines**: *(bool)*  
  Whether to allow quoted newlines in CSV data.

- **quote_character**: *(str)*  
  The character used for quoting in CSV data.

- **autodetect**: *(bool)*  
  Whether to enable schema autodetection.

- **replace_linebreaker**: *(bool)*  
  Whether to replace line breakers in the data.

- **xml_line_size**: *(int)*  
  The line size to use when processing XML files.

- **field_delimiter**: *(str)*  
  The character used to separate fields in the data files.

- **skip_leading_rows**: *(int)*  
  The number of leading rows to skip in the data files.

- **skip_tail_rows**: *(int)*  
  The number of trailing rows to skip in the data files.

- **col_fix_width**: *(list)*  
  A list specifying fixed-width column sizes.

- **null_marker**: *(str)*  
  The marker used to represent null values in the data.

- **tmp_tbl_schema**: *(dict)*  
  The schema of the temporary table.

- **IMPERSONATION_CHAIN**: *(str)*  
  The service account email used for impersonation to authenticate BigQuery access.

- **file_encoding**: *(str)*  
  The encoding of the source data files.

- **auto_add_columns**: *(bool)*  
  Whether to automatically add missing columns during the load.

## Returns

- **None**:  
  The function does not return any value. It initiates the data load process into BigQuery.

## Authentication

- **Impersonated Credentials**:  
  The function uses `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to BigQuery resources without relying on default credentials.

## Functionality

1. **Set Up Impersonated Credentials**:
   - Obtains the default credentials using `google.auth.default()`.
   - Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the necessary scope `https://www.googleapis.com/auth/cloud-platform`.

2. **Load Data Using `load_temp_table`**:
   - Calls the `load_temp_table.load_table` function with all the provided parameters.
   - Passes the impersonated credentials (`tcreds`) to ensure secure access.
   - The `load_table` function handles the actual data loading into the temporary BigQuery table.

3. **Commented Out Code**:
   - Note that the code for retrieving credentials via `airflow_conn_id` is commented out, indicating that impersonated credentials are preferred or currently in use.

---

**Note**: This function is essential for data pipelines that require loading control data into BigQuery while utilizing secure authentication methods. It provides flexibility in handling various data formats and configurations.

# File Validation Check Function

## Overview

The `venv_file_validation_check` function is designed to perform file validation checks on a BigQuery table within a virtual environment in Airflow. It leverages impersonated credentials for secure access and utilizes the `file_validation_function` from the `etlQAPackage` to execute the validation process.

Additionally, the code dynamically creates a `PythonVirtualenvOperator` in an Airflow DAG to run this function, ensuring that all dependencies are properly isolated in a virtual environment.

## Purpose

- **File Validation**: Perform validation checks on a BigQuery table to ensure data integrity and correctness.
- **Secure Authentication**: Utilize impersonated credentials to securely access Google Cloud resources without exposing sensitive credentials.
- **Airflow Integration**: Seamlessly integrate the validation function into an Airflow DAG using the `PythonVirtualenvOperator`.

## Parameters

### `venv_file_validation_check` Function

- **work_table**: *(str)*  
  The name of the BigQuery table to validate.

- **job_id**: *(str)*  
  A unique identifier for the validation job.

- **source_fields**: *(dict)*  
  A dictionary mapping source fields for validation.

- **gcp_project**: *(str)*  
  The Google Cloud project ID where the BigQuery table resides.

- **airflow_conn_id**: *(str)*  
  The Airflow connection ID used to retrieve credentials (commented out in the code).

- **IMPERSONATION_CHAIN**: *(str)*  
  The service account email used for impersonation to authenticate BigQuery access.

### `PythonVirtualenvOperator`

- **task_id**: *(str)*  
  Unique task identifier in the DAG, formatted as `'file_validation_check_' + table_name`.

- **python_callable**: *(callable)*  
  The `venv_file_validation_check` function to execute.

- **python_version**: *(str)*  
  The Python version to use in the virtual environment (e.g., `'3.8'`).

- **requirements**: *(list)*  
  A list of Python package requirements needed for the function (e.g., `[FRAMEWORKS_VERSION]`).

- **system_site_packages**: *(bool)*  
  Whether to include system site packages in the virtual environment (`True` or `False`).

- **op_kwargs**: *(dict)*  
  A dictionary of keyword arguments to pass to `venv_file_validation_check`:
  - **work_table**: The BigQuery table to validate, formatted with `datahub_temp_table` and dynamic values from Airflow's XCom.
  - **job_id**: The job ID, retrieved from Airflow's XCom.
  - **source_fields**: The `source_target_mapping` dictionary.
  - **gcp_project**: The Google Cloud project ID, retrieved from environment variables.
  - **airflow_conn_id**: An empty string or the Airflow connection ID (commented out in the code).
  - **IMPERSONATION_CHAIN**: The service account email used for impersonation.

## Returns

- **None**:  
  The function does not return any value. It performs validation checks and raises exceptions if any issues are found.

## Authentication

- **Impersonated Credentials**:  
  Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to BigQuery resources without using default credentials directly.

## Functionality

1. **Impersonation Setup**:
   - **Default Credentials**: Obtains default credentials and project ID using `google.auth.default()`.
   - **Impersonated Credentials**: Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the scope `https://www.googleapis.com/auth/cloud-platform`.

2. **Import Validation Module**:
   - Imports `file_validation` from `etlQAPackage`.

3. **Execute File Validation**:
   - Calls `file_validation.file_validation_function` with the provided parameters and impersonated credentials (`tcreds`).

4. **Handle `source_target_mapping` Parameter**:
   - Checks if `'source_target_mapping'` exists in the `parameter` dictionary.
   - If it doesn't exist, initializes `source_target_mapping` as an empty dictionary.

5. **Create Airflow Task**:
   - Uses `PythonVirtualenvOperator` to create a task named `'file_validation_check_' + table_name`.
   - Configures the operator to run `venv_file_validation_check` in a Python virtual environment.
   - Specifies Python version, requirements, and whether to include system site packages.
   - Passes necessary arguments via `op_kwargs`, including dynamic values pulled from Airflow's XCom and environment variables.

6. **Airflow DAG Integration**:
   - The operator is added to the DAG (`dag=dag`) to be executed as part of the Airflow workflow.

---

**Note**: This setup ensures that file validation is performed securely and efficiently within the Airflow ecosystem, leveraging virtual environments for dependency isolation and impersonated credentials for secure authentication.

# Auditing Check Function

## Overview

The `venv_auditing_check` function performs an auditing check between a control table and a work table in Google BigQuery. It uses impersonated credentials for secure authentication and runs within a virtual environment in Airflow using the `PythonVirtualenvOperator`. This function ensures data integrity by comparing counts and groupings between the control and work tables.

## Purpose

- **Data Validation**: Validate data consistency and integrity between the control table and the work table.
- **Auditing**: Perform auditing checks based on specified grouping and count columns to ensure data accuracy.
- **Secure Access**: Utilize impersonated credentials for secure access to Google Cloud resources.
- **Airflow Integration**: Seamlessly integrate the auditing process within an Airflow DAG using a virtual environment operator.

## Parameters

### `venv_auditing_check` Function Parameters

- **control_table**: *(str)*  
  The name of the control table in BigQuery used for auditing.

- **work_table**: *(str)*  
  The name of the work table in BigQuery to be audited.

- **group_by_column**: *(str)*  
  The column name used for grouping in the auditing process.

- **cnt_column**: *(str)*  
  The column name used for counting in the auditing process.

- **job_id**: *(str)*  
  A unique identifier for the auditing job.

- **gcp_project**: *(str)*  
  The Google Cloud project ID where the BigQuery tables reside.

- **label_key**: *(str)*  
  A key for labeling the BigQuery job.

- **label_value**: *(str)*  
  A value associated with the label key for the BigQuery job.

- **json_config**: *(dict)*  
  JSON configuration parameters for the auditing check.

- **airflow_conn_id**: *(str)*  
  The Airflow connection ID for retrieving credentials (currently not used; set to an empty string).

- **IMPERSONATION_CHAIN**: *(str)*  
  The service account email used for impersonation to authenticate BigQuery access.

### `PythonVirtualenvOperator` Parameters

- **task_id**: *(str)*  
  Unique task identifier in the Airflow DAG, formatted as `'auditing_check_' + table_name`.

- **python_callable**: *(callable)*  
  The `venv_auditing_check` function to execute.

- **python_version**: *(str)*  
  The Python version to use in the virtual environment (e.g., `'3.8'`).

- **requirements**: *(list)*  
  A list of Python package requirements needed for the function (e.g., `[FRAMEWORKS_VERSION]`).

- **system_site_packages**: *(bool)*  
  Whether to include system site packages in the virtual environment (`True` or `False`).

- **op_kwargs**: *(dict)*  
  A dictionary of keyword arguments to pass to `venv_auditing_check`:
  - **work_table**: The BigQuery work table to audit, formatted with dynamic values from Airflow's XCom.
  - **control_table**: The BigQuery control table, also formatted dynamically.
  - **group_by_column**: The column used for grouping, extracted from parameters.
  - **cnt_column**: The count column, extracted from parameters.
  - **label_key**: The label key for BigQuery job labeling.
  - **label_value**: The label value, typically the pipeline name.
  - **job_id**: The job identifier, retrieved from Airflow's XCom.
  - **gcp_project**: The Google Cloud project ID, retrieved from environment variables.
  - **json_config**: Auditing configuration details.
  - **airflow_conn_id**: An empty string (credentials are provided via impersonation).
  - **IMPERSONATION_CHAIN**: The service account email used for impersonation.

- **dag**:  
  The Airflow DAG to which this operator belongs.

## Returns

- **None**:  
  The function does not return any value. It performs the auditing check and raises exceptions if any discrepancies are found.

## Authentication

- **Impersonated Credentials**:  
  Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to BigQuery resources.

## Functionality

1. **Impersonation Setup**:
   - Retrieves default credentials and project ID using `google.auth.default()`.
   - Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the necessary scope `https://www.googleapis.com/auth/cloud-platform`.

2. **Import Auditing Module**:
   - Imports `auditingCheck` from `etlQAPackage`.

3. **Execute Auditing Check**:
   - Calls `auditingCheck.auditing_check` with the provided parameters and impersonated credentials (`tcreds`).
   - The auditing function compares data between the control and work tables based on the specified grouping and counting columns.

4. **Airflow Task Creation**:
   - A `PythonVirtualenvOperator` named `'auditing_check_' + table_name` is created.
   - Configured to run `venv_auditing_check` in a Python virtual environment.
   - Specifies Python version, required packages, and whether to include system site packages.
   - Passes necessary arguments via `op_kwargs`, including dynamic values pulled from Airflow's XCom and environment variables.

5. **DAG Integration**:
   - The operator is added to the DAG (`dag=dag`) to be executed as part of the Airflow workflow.

---

**Note**: This function is crucial for maintaining data quality and integrity within data pipelines by ensuring that the data in the work table matches expected results defined in the control table. Any discrepancies identified during the auditing process can be addressed promptly to prevent downstream data issues.

# DNTL Export Task Function

## Overview

The `venv_dntl_export_task` function is designed to perform a DNTL (Data Normalization and Transformation Layer) export operation within an Airflow DAG. It leverages impersonated credentials for secure access to Google Cloud resources and calls the `dntl_export` function from the `etlPackage` to execute the export process.

## Purpose

- **DNTL Export**: Executes the DNTL export process by transforming and exporting data from staging and working tables.
- **Secure Authentication**: Utilizes impersonated credentials to securely access Google Cloud services without exposing sensitive credentials.
- **Airflow Integration**: Integrates seamlessly into an Airflow DAG, allowing for dynamic task creation and execution.

## Parameters

- **stg_table**: *(str)*  
  The name of the staging table in BigQuery from which data is exported.

- **work_table**: *(str)*  
  The name of the working table in BigQuery used during the export process.

- **job_id**: *(str)*  
  A unique identifier for the DNTL export job.

- **label_key**: *(str)*  
  A key for labeling the BigQuery job.

- **label_value**: *(str)*  
  A value associated with the label key for the BigQuery job.

- **input_column_mapping**: *(dict)*  
  A dictionary mapping input columns for the export process.

- **tgt_dntl_flg**: *(bool)*  
  A flag indicating whether to target DNTL processing.

- **tgt_dntl_column_mapping**: *(dict)*  
  A dictionary mapping target DNTL columns.

- **tgt_dntl_export_flag**: *(bool)*  
  A flag indicating whether to export DNTL data.

- **tgt_dntl_export_columns**: *(list)*  
  A list of columns to export in the DNTL process.

- **gcp_project**: *(str)*  
  The Google Cloud project ID where the BigQuery tables reside.

- **export_columns**: *(list)*  
  A list of columns to be exported.

- **airflow_conn_id**: *(str)*  
  The Airflow connection ID for retrieving credentials (currently not used; commented out in the code).

- **IMPERSONATION_CHAIN**: *(str)*  
  The service account email used for impersonation to authenticate Google Cloud access.

## Returns

- **None**:  
  The function does not return any value. It initiates the DNTL export process using the provided parameters.

## Authentication

- **Impersonated Credentials**:  
  Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to Google Cloud resources without directly using default credentials.

## Functionality

1. **Import Necessary Modules**:
   - Imports `dntl_export` from the `etlPackage`.
   - Imports `google.auth` and `google.auth.impersonated_credentials` for authentication purposes.

2. **Set Up Impersonated Credentials**:
   - Retrieves default credentials and project ID using `google.auth.default()`.
   - Creates impersonated credentials (`tcreds`) targeting the `IMPERSONATION_CHAIN` service account with the scope `https://www.googleapis.com/auth/cloud-platform`.

3. **Execute DNTL Export**:
   - Calls `dntl_export.dntl_export` with the provided parameters and the impersonated credentials (`tcreds`).
   - The function handles the data export from the staging table to the target destination, applying any necessary transformations and mappings.

4. **Commented Out Code**:
   - The line using `get_creds_from_conn_id` is commented out, indicating that the function currently relies on impersonated credentials rather than retrieving credentials via Airflow's connection ID.

---

**Note**: This function is essential for data pipelines that require exporting and transforming data securely within the Google Cloud environment. By using impersonated credentials and integrating with Airflow, it ensures both security and scalability in data processing workflows.

# DNTL Task Function

## Overview

The `venv_dntl_task` function performs a Data Normalization and Transformation Layer (DNTL) operation within an Airflow DAG. It sets up impersonated credentials for secure access to Google Cloud resources and invokes the `dntl` function from the `etlPackage` to execute the data transformation process.

## Purpose

- **Data Transformation**: Transforms data from a staging table to a work table in BigQuery using specified mappings and configurations.
- **Secure Authentication**: Utilizes impersonated credentials to securely access Google Cloud services without exposing sensitive credentials.
- **Airflow Integration**: Seamlessly integrates into an Airflow DAG, allowing for dynamic task creation and execution in a virtual environment.

## Parameters

- **stg_table** *(str)*: The name of the staging table in BigQuery from which data is read.
- **work_table** *(str)*: The name of the work table in BigQuery where transformed data is written.
- **job_id** *(str)*: A unique identifier for the DNTL job.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for the BigQuery job.
- **source_fields** *(dict)*: A dictionary mapping source fields for the transformation process.
- **input_column_mapping** *(dict)*: A dictionary mapping input columns for the DNTL process.
- **tgt_dntl_flg** *(bool)*: A flag indicating whether to perform target DNTL processing.
- **tgt_dntl_column_mapping** *(dict)*: A dictionary mapping target DNTL columns.
- **gcp_project** *(str)*: The Google Cloud project ID where the BigQuery tables reside.
- **json_config** *(dict)*: JSON configuration parameters for the DNTL process.
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials (currently not used; commented out in the code).
- **IMPERSONATION_CHAIN** *(str)*: The service account email used for impersonation to authenticate Google Cloud access.

## Returns

- **None**: The function does not return any value. It initiates the DNTL process using the provided parameters.

## Authentication

- **Impersonated Credentials**: Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to Google Cloud resources without directly using default credentials.

## Functionality

1. **Import Necessary Modules**:
   - Imports `dntl` from the `etlPackage`.
   - Imports `google.auth` and `google.auth.impersonated_credentials` for authentication purposes.

2. **Set Up Impersonated Credentials**:
   - Retrieves default credentials and project ID using `google.auth.default()`.
   - Creates impersonated credentials (`tcreds`) targeting the `IMPERSONATION_CHAIN` service account with the required scope.

3. **Execute DNTL Process**:
   - Calls `dntl.dntl` with the provided parameters and the impersonated credentials (`tcreds`).
   - Handles data transformation from the staging table to the work table, applying the specified mappings and configurations.

4. **Commented Out Code**:
   - The line using `get_creds_from_conn_id` is commented out, indicating that the function relies on impersonated credentials instead of retrieving credentials via Airflow's connection ID.

---

**Note**: This function is essential for data pipelines that require secure and efficient data transformation within the Google Cloud environment. By leveraging impersonated credentials and integrating with Airflow, it ensures both security and scalability in data processing workflows.

# DLP Temporary Table Loader Function

## Overview

This section defines the `venv_load_dlp_tmp_table` function, which is responsible for loading a temporary table for Data Loss Prevention (DLP) processing in Google BigQuery. It leverages impersonated credentials for secure access and integrates with Airflow to handle batch processing limits.

## Purpose

- **Load DLP Temporary Table**: Prepares and loads a temporary BigQuery table with the necessary data for DLP operations.
- **Handle Batch Payload Limits**: Adjusts the batch payload limit based on configuration parameters to optimize processing.
- **Secure Authentication**: Uses impersonated credentials to securely access Google Cloud resources.

## Parameters

- **dlp_tmp_table** *(str)*: The name of the temporary DLP table in BigQuery.
- **dlp_tmp_table_with_pk** *(str)*: The name of the temporary DLP table with primary key included.
- **dlp_column** *(str)*: The column in the table that requires DLP processing.
- **job_id** *(str)*: A unique identifier for the DLP load job.
- **dlp_condition** *(str)*: The condition or query filter to apply during data loading.
- **gcp_project** *(str)*: The Google Cloud project ID where the BigQuery tables reside.
- **expiration_hours** *(int)*: The number of hours after which the temporary table will expire.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for the BigQuery job.
- **json_config** *(dict)*: JSON configuration parameters for the data load.
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials (currently not used; commented out).
- **IMPERSONATION_CHAIN** *(str)*: The service account email used for impersonation to authenticate Google Cloud access.

## Returns

- **None**: The function does not return any value. It initiates the loading of the DLP temporary table using the provided parameters.

## Authentication

- **Impersonated Credentials**: Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to Google Cloud resources without directly using default credentials.

## Functionality

1. **Batch Payload Limit Determination**:
   - Checks if `batch_payload_limit` is specified in `parameter['dlp']`.
   - If specified, uses that value; otherwise, defaults to `master_batch_payload_limit`.
   - Logs the batch payload limit used, especially in development (`dv`) or quality assurance (`qa`) environments.

2. **Define `venv_load_dlp_tmp_table` Function**:
   - Imports necessary modules, including `load_temp_table` from `etlPackage` and Google authentication libraries.
   - Sets up impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the necessary scope.

3. **Load DLP Temporary Table**:
   - Calls `load_temp_table.load_dlp_tmp_table` with the provided parameters and the impersonated credentials (`tcreds`).
   - The function handles the loading of data into a temporary BigQuery table for DLP processing, applying any specified conditions.

4. **Commented Out Code**:
   - The line using `get_creds_from_conn_id` is commented out, indicating reliance on impersonated credentials rather than Airflow's connection ID for authentication.

---
# WritetoBQ Task Function

## Overview

The `venv_writetobq_task` function is designed to write data from a temporary work table to a BigQuery staging table within an Airflow pipeline. It utilizes impersonated credentials for secure access to Google Cloud resources and integrates with Airflow using the `PythonVirtualenvOperator`. The function handles various configurations such as column mappings, additional columns, and disposition types to manage how data is written to BigQuery.

## Purpose

- **Data Loading**: Transfers data from a temporary work table to a BigQuery staging table.
- **Configuration Flexibility**: Allows specifying how data should be written, including append or overwrite dispositions.
- **Column Mapping**: Supports mapping between source and target columns, including handling of additional columns if required.
- **Secure Access**: Utilizes impersonated credentials to securely access BigQuery without exposing sensitive credentials.

## Parameters

- **write2bq_table** *(str)*: The target BigQuery staging table where data will be written.
- **work_table** *(str)*: The source BigQuery temporary work table containing data to be written.
- **source_target_mapping** *(dict)*: A dictionary mapping source columns to target columns.
- **disposition_type** *(str)*: Specifies the write disposition (e.g., `'WRITE_APPEND'` or `'WRITE_TRUNCATE'`).
- **additional_columns_flag** *(bool)*: Flag indicating whether to include additional columns in the target table.
- **file_name** *(str)*: The name of the file being processed, often used for logging or tracking purposes.
- **create_ts** *(str)*: Timestamp indicating when the record was created.
- **create_user_id** *(str)*: User ID of the creator, typically the service account.
- **last_updt_ts** *(str)*: Timestamp of the last update.
- **last_updt_user_id** *(str)*: User ID who last updated the record.
- **label_key** *(str)*: Key for labeling the BigQuery job.
- **label_value** *(str)*: Value associated with the label key for job tracking.
- **gcp_project** *(str)*: Google Cloud project ID where BigQuery tables reside.
- **job_id** *(str)*: Unique identifier for the job.
- **json_config** *(dict)*: JSON configuration details for auditing or logging.
- **airflow_conn_id** *(str)*: Airflow connection ID for retrieving credentials (currently set to an empty string).
- **additional_conditions** *(str)*: Additional SQL conditions to apply during data write.
- **IMPERSONATION_CHAIN** *(str)*: Service account email used for impersonation to authenticate BigQuery access.
- **auto_add_columns** *(str)*: Specifies whether to automatically add columns if they are missing (`'add'` or `'no_add'`).

## Returns

- **None**: The function does not return a value; it performs the data write operation to BigQuery.

## Authentication

- **Impersonated Credentials**: Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate a service account specified by `IMPERSONATION_CHAIN`. This allows secure access to BigQuery resources without using default credentials directly.

## Functionality

1. **Impersonation Setup**:
   - Imports necessary modules, including `google.auth` and `google.auth.impersonated_credentials`.
   - Sets up impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the scope `'https://www.googleapis.com/auth/cloud-platform'`.

2. **Determine Auto-Add Columns**:
   - Checks if `'auto_add_columns'` is specified in the `parameter` dictionary.
   - Sets `auto_add_columns` to `'add'` if `parameter['auto_add_columns']` is `True`, otherwise defaults to `'no_add'`.

3. **Invoke Write Function**:
   - Calls `write2bq.writetobq` with all provided parameters and impersonated credentials.
   - The function handles writing data from the work table to the BigQuery staging table, applying any additional conditions and handling column mappings.

4. **Airflow Operator Creation**:
   - Creates a `PythonVirtualenvOperator` named `'writetobq_task_<table_name>'`.
   - Configures the operator to run `venv_writetobq_task` in a Python virtual environment with specified requirements.
   - Passes necessary arguments via `op_kwargs`, including dynamic values retrieved from Airflow's XCom and environment variables.

5. **DAG Integration**:
   - Adds the operator to the Airflow DAG (`dag=dag`) for execution as part of the workflow.

---

**Note**: This function is crucial for data pipelines that require moving transformed data into BigQuery for further processing or storage. By using impersonated credentials and integrating with Airflow, it ensures secure and efficient data operations within the Google Cloud environment.

# WritetoSource Task Function

## Overview

The `venv_writetosource_task` function is designed to write data from a temporary work table to a specified source BigQuery table within an Airflow pipeline. It utilizes impersonated credentials for secure access to Google Cloud resources and integrates with Airflow using the `PythonVirtualenvOperator`. The function handles configurations such as column mappings, write dispositions, and additional columns to manage how data is written to the source table.

## Purpose

- **Data Loading**: Transfers data from a temporary work table to a specified source BigQuery table.
- **Configuration Flexibility**: Allows specification of write dispositions (e.g., `'WRITE_APPEND'` or `'WRITE_TRUNCATE'`), and whether to auto-add columns.
- **Column Mapping**: Supports mapping between source and target columns, and handling additional columns if required.
- **Secure Access**: Utilizes impersonated credentials to securely access BigQuery without exposing sensitive credentials.

## Parameters

### `venv_writetosource_task` Function Parameters

- **write2bq_table** *(str)*: The target BigQuery source table where data will be written.
- **work_table** *(str)*: The source BigQuery temporary work table containing data to be written.
- **source_target_mapping** *(dict)*: A dictionary mapping source columns to target columns.
- **disposition_type** *(str)*: Specifies the write disposition (e.g., `'WRITE_APPEND'` or `'WRITE_TRUNCATE'`).
- **additional_columns_flag** *(bool)*: Flag indicating whether to include additional columns in the target table.
- **file_name** *(str)*: The name of the file being processed, often used for logging or tracking purposes.
- **create_ts** *(str)*: Timestamp indicating when the record was created.
- **create_user_id** *(str)*: User ID of the creator, typically the service account.
- **last_updt_ts** *(str)*: Timestamp of the last update.
- **last_updt_user_id** *(str)*: User ID who last updated the record.
- **label_key** *(str)*: Key for labeling the BigQuery job.
- **label_value** *(str)*: Value associated with the label key for job tracking.
- **gcp_project** *(str)*: Google Cloud project ID where BigQuery tables reside.
- **job_id** *(str)*: Unique identifier for the job.
- **json_config** *(dict)*: JSON configuration details for auditing or logging.
- **airflow_conn_id** *(str)*: Airflow connection ID for retrieving credentials (currently set to an empty string).
- **IMPERSONATION_CHAIN** *(str)*: Service account email used for impersonation to authenticate BigQuery access.
- **auto_add_columns** *(str)*: Specifies whether to automatically add columns if they are missing (`'add'` or `'no_add'`).

### Other Parameters

- **parameter** *(dict)*: Configuration dictionary containing various settings for the pipeline.
- **processing_modules** *(list)*: List of processing modules included in the pipeline.
- **sourcelayer_project** *(str)*: The project where the source layer table resides, determined from `parameter['sourcelayer_project']`.
- **table_name** *(str)*: The name of the table being processed, used for dynamic task ID generation.
- **FRAMEWORKS_VERSION** *(str)*: The version of the required Python frameworks.
- **dag**: The Airflow DAG object to which the operator is added.

## Returns

- **None**: The function does not return a value; it performs the data write operation to the source BigQuery table.

## Authentication

- **Impersonated Credentials**: Uses `google.auth.impersonated_credentials.Credentials` to impersonate a service account specified by `IMPERSONATION_CHAIN`. This allows secure access to BigQuery resources without using default credentials directly.

## Functionality

1. **Check for 'writetosource_task' in Processing Modules**:
   - The code first checks if `'writetosource_task'` is present in the `processing_modules` list to determine whether to execute this task.

2. **Define `venv_writetosource_task` Function**:
   - This function imports necessary modules and sets up impersonated credentials.
   - Calls `write2bq.writetobq` with the provided parameters to write data to BigQuery.

3. **Determine Source Layer Project**:
   - Retrieves `write2bq_table` and `sourcelayer_project` from the `parameter` dictionary.
   - Resolves `sourcelayer_project` to the appropriate project ID based on environment variables (`AIRFLOW_VAR_WORK_PROJECT` or `AIRFLOW_VAR_ENT_PROJECT`).
   - Constructs the full table name by combining `sourcelayer_project` and `write2bq_table`.

4. **Set Auto-Add Columns Flag**:
   - Checks if `'auto_add_columns'` is specified in the `parameter` dictionary.
   - Sets `auto_add_columns` to `'add'` if `parameter['auto_add_columns']` is `True`; otherwise, it defaults to `'no_add'`.

5. **Create Airflow Task ID**:
   - Generates a unique task ID `tid` using the table name.

6. **Create PythonVirtualenvOperator**:
   - A `PythonVirtualenvOperator` is instantiated with the task ID `tid`.
   - Configured to run `venv_writetosource_task` in a Python virtual environment with specified requirements.
   - Passes necessary arguments via `op_kwargs`, including dynamic values retrieved from Airflow's XCom and environment variables.

7. **DAG Integration**:
   - The operator is added to the Airflow DAG (`dag=dag`) for execution as part of the workflow.

---

**Note**: This function is essential for data pipelines that require moving transformed data into a source BigQuery table for further processing or storage. By using impersonated credentials and integrating with Airflow, it ensures secure and efficient data operations within the Google Cloud environment.

# Deduplication of Source Layer Task

## Overview

The code snippet defines a function `venv_sourcelayerdedup_task` which performs deduplication on a BigQuery source layer table within an Airflow DAG. It utilizes impersonated credentials for secure access to Google Cloud resources and dynamically creates a `PythonVirtualenvOperator` to execute the deduplication process in a virtual environment.

## Purpose

- **Data Deduplication**: Removes duplicate records from a specified BigQuery table based on certain columns.
- **Dynamic Task Creation**: Integrates the deduplication process into an Airflow DAG by creating tasks at runtime.
- **Secure Authentication**: Uses impersonated credentials to securely interact with BigQuery services.

## Parameters

### Function Parameters (`venv_sourcelayerdedup_task`)

- **work_table** *(str)*: The BigQuery table to perform deduplication on.
- **job_id** *(str)*: A unique identifier for the deduplication job.
- **label_key** *(str)*: A key used for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for job tracking.
- **staging_table** *(str)*: The BigQuery staging table used during the deduplication process.
- **dedup_column** *(str)*: The column name used to identify duplicates.
- **partition_column** *(str)*: The column used for partitioning data during deduplication.
- **order_by** *(str)*: The column(s) used to order data when removing duplicates.
- **gcp_project** *(str)*: The Google Cloud project ID where BigQuery tables reside.
- **json_config** *(dict)*: Configuration details in JSON format for auditing or logging purposes.
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials (currently set to an empty string).
- **IMPERSONATION_CHAIN** *(str)*: Service account email used for impersonation to authenticate BigQuery access.

### Additional Variables

- **processing_modules** *(list)*: A list of processing modules to determine if the deduplication task should be executed.
- **parameter** *(dict)*: Configuration dictionary containing various pipeline settings.
- **sourcelayer_project** *(str)*: The project ID where the source layer table resides, determined based on environment variables.
- **write2bq_table** *(str)*: The full BigQuery table name for the source layer table, including project and dataset.
- **table_name** *(str)*: The name of the table being processed, used to create unique task IDs.
- **FRAMEWORKS_VERSION** *(str)*: The version of the required Python frameworks.
- **dag**: The Airflow DAG object to which the operator is added.

## Returns

- **None**: The function does not return any value; it performs the deduplication operation on the specified BigQuery table.

## Authentication

- **Impersonated Credentials**: The function uses `google.auth.impersonated_credentials.Credentials` to impersonate a target service account specified by `IMPERSONATION_CHAIN`. This allows for secure and scoped access to Google Cloud resources.

## Functionality

1. **Check for Deduplication Task**:
   - The code checks if `'dedup_sourcelayer_task'` is present in the `processing_modules` list. If it is, the deduplication task is executed.

2. **Define `venv_sourcelayerdedup_task` Function**:
   - Imports necessary modules, including `dedup_target` from `etlPackage`, and Google authentication libraries.
   - Sets up impersonated credentials using the `IMPERSONATION_CHAIN` service account.

3. **Set Up Impersonated Credentials**:
   - Retrieves default credentials and project ID using `google.auth.default()`.
   - Creates impersonated credentials with the required scope `'https://www.googleapis.com/auth/cloud-platform'`.

4. **Execute Deduplication Process**:
   - Calls `dedup_target.dedup` with the provided parameters and impersonated credentials to perform the deduplication on the `work_table`.

5. **Determine Source Layer Project**:
   - Retrieves `write2bq_table` and `sourcelayer_project` from the `parameter` dictionary.
   - Resolves `sourcelayer_project` to the appropriate project ID based on environment variables (`AIRFLOW_VAR_WORK_PROJECT` or `AIRFLOW_VAR_ENT_PROJECT`).
   - Constructs the full table name by combining `sourcelayer_project` and `write2bq_table`.

6. **Create Airflow Task ID**:
   - Generates a unique task ID `tid` using the `table_name`.

7. **Create `PythonVirtualenvOperator`**:
   - A `PythonVirtualenvOperator` is instantiated with the task ID `tid`.
   - Configured to run `venv_sourcelayerdedup_task` in a Python virtual environment with specified requirements.
   - Passes necessary arguments via `op_kwargs`, including dynamic values retrieved from Airflow's XCom and environment variables.

8. **DAG Integration**:
   - The operator is added to the Airflow DAG (`dag=dag`) to be executed as part of the workflow.

---

**Note**: This function is crucial for maintaining data integrity within data pipelines by removing duplicate records from source layer tables in BigQuery. By utilizing impersonated credentials and integrating with Airflow, it ensures secure and efficient data processing within the Google Cloud environment.

# Retention Source Task Function

## Overview

The `venv_source_retention_task` function performs data retention operations on a specified BigQuery table within an Airflow DAG. It utilizes impersonated credentials for secure access to Google Cloud resources and dynamically constructs the retention query based on environment variables and parameters.

## Purpose

- **Data Retention**: Executes retention queries to delete or archive data from a BigQuery table based on defined conditions.
- **Dynamic Configuration**: Customizes the retention query and target table using environment variables and DAG parameters.
- **Secure Authentication**: Employs impersonated credentials to securely interact with Google Cloud services without exposing sensitive credentials.

## Parameters

### `venv_source_retention_task` Function Parameters

- **job_id** *(str)*: A unique identifier for the retention job.
- **staging_table** *(str)*: The BigQuery table on which the retention operation will be performed.
- **retention_query** *(str)*: The SQL query defining the retention logic to be applied.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for job tracking.
- **gcp_project** *(str)*: The Google Cloud project ID where BigQuery tables reside.
- **json_config** *(dict)*: Configuration details in JSON format for auditing or logging purposes.
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials (currently set to an empty string).
- **IMPERSONATION_CHAIN** *(str)*: Service account email used for impersonation to authenticate BigQuery access.

### Additional Variables

- **processing_modules** *(list)*: A list of processing modules to determine if the retention task should be executed.
- **parameter** *(dict)*: Configuration dictionary containing various pipeline settings.
- **ent_project** *(str)*: Enterprise project ID, retrieved from environment variables.
- **work_project** *(str)*: Work project ID, retrieved from environment variables.
- **retention_source_query** *(str)*: The retention query with placeholders replaced by actual project IDs.
- **write2bq_table** *(str)*: The full BigQuery table name for the source layer table, including project and dataset.
- **sourcelayer_project** *(str)*: The project ID where the source layer table resides.
- **table_name** *(str)*: The name of the table being processed, used to create unique task IDs.
- **FRAMEWORKS_VERSION** *(str)*: The version of the required Python frameworks.
- **dag**: The Airflow DAG object to which the operator is added.

## Returns

- **None**: The function does not return any value; it performs the retention operation on the specified BigQuery table.

## Authentication

- **Impersonated Credentials**: The function uses `google.auth.impersonated_credentials.Credentials` to impersonate a target service account specified by `IMPERSONATION_CHAIN`. This allows for secure and scoped access to Google Cloud resources.

## Functionality

1. **Check for Retention Task**:
   - The code checks if `'retention_source_task'` is present in the `processing_modules` list. If it is, the retention task is executed.

2. **Define `venv_source_retention_task` Function**:
   - Imports necessary modules, including `retention` from `etlPackage`, and Google authentication libraries.
   - Sets up impersonated credentials using the `IMPERSONATION_CHAIN` service account.
   - Calls `retention.retention` with the provided parameters to perform the retention operation on the `staging_table`.

3. **Set Up Impersonated Credentials**:
   - Retrieves default credentials and project ID using `google.auth.default()`.
   - Creates impersonated credentials with the required scope `'https://www.googleapis.com/auth/cloud-platform'`.

4. **Prepare Retention Query**:
   - Retrieves `ent_project` and `work_project` from environment variables.
   - Obtains `retention_source_query` from the `parameter` dictionary.
   - Replaces placeholders `{work}` and `{enterprise}` in the `retention_source_query` with actual project IDs.

5. **Determine Source Layer Project**:
   - Retrieves `write2bq_table` and `sourcelayer_project` from the `parameter` dictionary.
   - Resolves `sourcelayer_project` to the appropriate project ID based on environment variables.
   - Constructs the full table name by combining `sourcelayer_project` and `write2bq_table`.

6. **Create Airflow Task ID**:
   - Generates a unique task ID `tid` using the `table_name`.

7. **Create `PythonVirtualenvOperator`**:
   - Instantiates a `PythonVirtualenvOperator` with the task ID `tid`.
   - Configured to run `venv_source_retention_task` in a Python virtual environment with specified r

# Pre-Execution Task Function

## Overview

The `venv_pre_execution_task` function is designed to execute a series of ad-hoc SQL queries in Google BigQuery before the main data pipeline runs. It uses impersonated credentials for secure access and logs execution details for auditing purposes.

## Purpose

- **Ad-hoc Query Execution**: Runs custom SQL queries provided in `query_list` to prepare or manipulate data as needed before the main execution.
- **Dynamic Query Construction**: Replaces placeholders in the queries with actual project IDs and table names to adapt to different environments.
- **Logging and Auditing**: Logs execution details to Google Cloud Logging and sends audit information for compliance and tracking.

## Parameters

- **job_id** *(str)*: A unique identifier for the execution job.
- **staging_table** *(str)*: The BigQuery staging table involved in the queries.
- **query_list** *(list of str)*: A list of SQL queries to be executed.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for job tracking.
- **gcp_project** *(str)*: The Google Cloud project ID where BigQuery tables reside.
- **json_config** *(dict)*: JSON configuration for auditing.
- **work_table** *(str)*: The temporary work table name.
- **work_project** *(str)*: The Google Cloud project ID for the work environment.
- **ent_project** *(str)*: The Google Cloud project ID for the enterprise environment.
- **airflow_conn_id** *(str)*: Airflow connection ID for credentials (currently not used).
- **IMPERSONATION_CHAIN** *(str)*: Service account email used for impersonation to authenticate BigQuery access.

## Returns

- **bool**: Returns `True` upon successful execution of the queries.

## Authentication

- **Impersonated Credentials**: Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to Google Cloud resources without directly using default credentials.

## Functionality

1. **Set Up Impersonated Credentials**:
   - Retrieves default credentials using `google.auth.default()`.
   - Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the scope `https://www.googleapis.com/auth/cloud-platform`.

2. **Import Necessary Modules**:
   - Imports modules for BigQuery, Cloud Logging, and auditing.
   - Includes standard libraries such as `datetime`, `os`, and `logging`.

3. **Initialize Clients**:
   - Creates a BigQuery client and a Cloud Logging client using the impersonated credentials.

4. **Construct Ad-hoc Query**:
   - Joins the list of queries from `query_list` into a single string separated by semicolons.
   - Replaces placeholders `{work}`, `{enterprise}`, and `{tmp}` in the query with actual project IDs and table names.

5. **Logging Configuration**:
   - Sets up a logger named `adhoc_execution_logs`.
   - Configures logging levels based on the environment (e.g., development or QA).

6. **Execute BigQuery Job**:
   - Creates a `QueryJobConfig` with labels for tracking.
   - Sets `use_legacy_sql` to `False` to use standard SQL.
   - Submits the query to BigQuery and waits for the result.
   - Prints the results for debugging purposes.

7. **Audit Logging**:
   - Constructs a `qa_logs` dictionary with execution details such as job ID, process name, status, and timestamp.
   - Logs the structured data to Cloud Logging with severity `INFO`.
   - Calls `generic_send_audit` to send audit information.

8. **Return Statement**:
   - Returns `True` to indicate successful execution.

## Notes

- The function is intended to be executed within an Airflow DAG, potentially using a `PythonVirtualenvOperator`.
- Some imports and function calls are commented out, indicating they might be placeholders or alternative implementations.
- Environment variables like `AIRFLOW_VAR_ENV` are used to determine logging verbosity.
- The function handles both authentication and execution within itself, making it a self-contained pre-execution task.

# Post-Execution Task Function

## Overview

The `venv_postexecution_task` function is designed to execute a series of ad-hoc SQL queries in Google BigQuery after the main data pipeline has completed. It uses impersonated credentials for secure access and logs execution details for auditing purposes. This function is integrated into an Airflow DAG using the `PythonVirtualenvOperator`.

## Purpose

- **Ad-hoc Query Execution**: Runs custom SQL queries provided in `query_list` to perform post-processing tasks such as data cleanup, updates, or other necessary operations.
- **Dynamic Query Construction**: Replaces placeholders in the queries with actual project IDs and table names to adapt to different environments (e.g., development, QA, production).
- **Logging and Auditing**: Logs execution details to Google Cloud Logging and sends audit information for compliance and tracking purposes.

## Parameters

- **job_id** *(str)*: A unique identifier for the execution job.
- **staging_table** *(str)*: The BigQuery staging table involved in the queries.
- **query_list** *(list of str)*: A list of SQL queries to be executed.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for job tracking.
- **gcp_project** *(str)*: The Google Cloud project ID where BigQuery tables reside.
- **json_config** *(dict)*: JSON configuration for auditing.
- **work_table** *(str)*: The temporary work table name.
- **work_project** *(str)*: The Google Cloud project ID for the work environment.
- **ent_project** *(str)*: The Google Cloud project ID for the enterprise environment.
- **airflow_conn_id** *(str)*: Airflow connection ID for credentials (currently not used; set to an empty string).
- **IMPERSONATION_CHAIN** *(str)*: Service account email used for impersonation to authenticate BigQuery access.

## Returns

- **bool**: Returns `True` upon successful execution of the queries.

## Authentication

- **Impersonated Credentials**: Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`. This allows secure and scoped access to Google Cloud resources without directly using default credentials.

## Functionality

1. **Set Up Impersonated Credentials**:
   - Retrieves default credentials using `google.auth.default()`.
   - Creates impersonated credentials targeting the `IMPERSONATION_CHAIN` service account with the scope `https://www.googleapis.com/auth/cloud-platform`.

2. **Import Necessary Modules**:
   - Imports modules for BigQuery, Cloud Logging, and auditing.
   - Includes standard libraries such as `datetime`, `os`, and `logging`.

3. **Initialize Clients**:
   - Creates a BigQuery client and a Cloud Logging client using the impersonated credentials.

4. **Construct Ad-hoc Query**:
   - Joins the list of queries from `query_list` into a single string separated by semicolons.
   - Replaces placeholders `{work}`, `{enterprise}`, and `{tmp}` in the query with actual project IDs and table names.

5. **Logging Configuration**:
   - Sets up a logger named `adhoc_execution_logs`.
   - Configures logging levels based on the environment (e.g., development or QA).

6. **Execute BigQuery Job**:
   - Creates a `QueryJobConfig` with labels for tracking.
   - Sets `use_legacy_sql` to `False` to use standard SQL.
   - Submits the query to BigQuery and waits for the result.
   - Prints the results for debugging purposes.

7. **Audit Logging**:
   - Constructs a `qa_logs` dictionary with execution details such as job ID, process name, status, and timestamp.
   - Logs the structured data to Cloud Logging with severity `INFO`.
   - Calls `generic_send_audit` to send audit information.

8. **Return Statement**:
   - Returns `True` to indicate successful execution.

---
# Retention Task Function

## Overview

The `venv_retention_task` function performs data retention operations on a specified BigQuery table within an Airflow DAG. It uses impersonated credentials for secure access to Google Cloud resources and integrates with Airflow using the `PythonVirtualenvOperator`.

## Purpose

- **Data Retention**: Executes a retention query to manage data lifecycle in a BigQuery table.
- **Secure Authentication**: Utilizes impersonated credentials for secure access to Google Cloud services.
- **Airflow Integration**: Seamlessly integrates into an Airflow DAG for automated pipeline execution.

## Parameters

- **job_id** *(str)*: A unique identifier for the retention job.
- **staging_table** *(str)*: The BigQuery table on which the retention operation will be performed.
- **retention_query** *(str)*: The SQL query defining the retention logic to be applied.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for job tracking.
- **gcp_project** *(str)*: The Google Cloud project ID where BigQuery tables reside.
- **json_config** *(dict)*: Configuration details in JSON format for auditing or logging purposes.
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials.
  - **Optional**: Yes
  - **Default Value**: Empty string (`''`)
- **IMPERSONATION_CHAIN** *(str)*: The service account email used for impersonation to authenticate BigQuery access.

### Internal Variables

- **target_scopes** *(list)*: The OAuth scopes required for the impersonated credentials.
- **source_credentials** *(Credentials)*: The source credentials obtained from the default environment.
- **tcreds** *(Credentials)*: The impersonated credentials used for authentication.

## Returns

- **None**: The function does not return any value; it performs the retention operation.
- **Side Effects**: Executes a retention query on BigQuery, potentially deleting or archiving data.

## Authentication

- **Services Used**:
  - **Google Cloud BigQuery**: Executes SQL queries on BigQuery tables.
  - **Google Authentication Library**: Handles credential management.
- **Authentication Methods**:
  - **Impersonated Credentials**: Uses `google.auth.impersonated_credentials.Credentials` to impersonate a target service account specified by `IMPERSONATION_CHAIN`.
- **Permissions Required**:
  - The source service account must have the `roles/iam.serviceAccountTokenCreator` role on the target service account.
  - The target service account must have the necessary permissions to perform BigQuery operations.

## Functionality

1. **Set Up Impersonated Credentials**:
   - **Import Modules**: Imports necessary libraries for authentication and BigQuery operations.
   - **Define Scopes**: Sets `target_scopes` to `['https://www.googleapis.com/auth/cloud-platform']`.
   - **Obtain Default Credentials**: Calls `google.auth.default()` to get `source_credentials` and `project_id`.
   - **Create Impersonated Credentials**: Uses `google.auth.impersonated_credentials.Credentials` with `source_credentials`, `target_principal` set to `IMPERSONATION_CHAIN`, and `target_scopes`.

2. **Execute Retention Query**:
   - **Import Retention Module**: Imports the `retention` function from `etlPackage`.
   - **Call Retention Function**: Executes `retention.retention()` with the provided parameters and `credentials=tcreds`.
     - **Parameters Passed**: Includes `job_id`, `staging_table`, `retention_query`, `label_key`, `label_value`, `gcp_project`, `json_config`, and `credentials`.

3. **Integrate with Airflow DAG**:
   - **Create Operator**: Uses `PythonVirtualenvOperator` to create a task named `'retention_task_' + str(table_name)`.
   - **Configure Operator**:
     - **Python Callable**: Sets to `venv_retention_task`.
     - **Python Version**: Specifies `'3.8'`.
     - **Requirements**: Includes necessary frameworks via `FRAMEWORKS_VERSION`.
     - **System Site Packages**: Sets `system_site_packages=True` to include system packages.
     - **Operational Arguments**: Passes `op_kwargs` with all required parameters, including dynamic values from Airflow's XCom and environment variables.
   - **Add to DAG**: Attaches the operator to the Airflow DAG for execution.

4. **Error Handling**:
   - **Assumption of Success**: The function assumes successful execution; any exceptions will be managed by Airflow's task failure mechanisms.
   - **No Explicit Error Handling**: Does not include try-except blocks within the function.

5. **Edge Cases**:
   - **Invalid Credentials**: If impersonated credentials cannot be obtained, the function will fail when attempting to execute the retention query.
   - **Invalid Query or Table**: Does not handle cases where `retention_query` is invalid or `staging_table` does not exist.

---

**Note**: This function is crucial for managing data lifecycle policies within data pipelines, ensuring that outdated or unnecessary data is properly removed or archived according to specified retention policies.

# Duplication Check Function

## Overview

The `venv_duplication_check` function performs a duplication check on a specified BigQuery table within an Airflow DAG. It utilizes impersonated credentials for secure access to Google Cloud resources and integrates with Airflow using the `PythonVirtualenvOperator`.

## Purpose

- **Data Quality Assurance**: Ensures data integrity by identifying duplicate records in a BigQuery table.
- **Automated Pipeline Integration**: Seamlessly incorporates duplication checks into data processing pipelines within Airflow.
- **Secure Authentication**: Uses impersonated credentials to securely interact with Google Cloud services.

## Parameters

- **work_table** *(str)*: The BigQuery table on which the duplication check will be performed.
- **job_id** *(str)*: A unique identifier for the duplication check job.
- **gcp_project** *(str)*: The Google Cloud project ID where the BigQuery table resides.
- **label_key** *(str)*: A key for labeling the BigQuery job.
- **label_value** *(str)*: A value associated with the label key for job tracking.
- **input_columns** *(list)*: A list of column names used to identify duplicates.
- **threshold** *(int)*: The maximum allowable number of duplicate records.
- **json_config** *(dict)*: Configuration details in JSON format for auditing or logging purposes.
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials.
  - **Optional**: Yes
  - **Default Value**: Empty string (`''`)
- **IMPERSONATION_CHAIN** *(str)*: The service account email used for impersonation to authenticate BigQuery access.

### Internal Variables

- **target_scopes** *(list)*: The OAuth scopes required for the impersonated credentials.
- **source_credentials** *(Credentials)*: The source credentials obtained from the default environment.
- **tcreds** *(Credentials)*: The impersonated credentials used for authentication.

## Returns

- **None**: The function does not return any value; it performs the duplication check operation.
- **Side Effects**: Executes a duplication check on BigQuery, potentially raising exceptions if duplicates exceed the threshold.

## Authentication

- **Services Used**:
  - **Google Cloud BigQuery**: Executes SQL queries on BigQuery tables.
  - **Google Authentication Library**: Handles credential management.
- **Authentication Methods**:
  - **Impersonated Credentials**: Uses `google.auth.impersonated_credentials.Credentials` to impersonate a target service account specified by `IMPERSONATION_CHAIN`.
- **Permissions Required**:
  - The source service account must have the `roles/iam.serviceAccountTokenCreator` role on the target service account.
  - The target service account must have the necessary permissions to perform BigQuery operations.

## Functionality

1. **Set Up Impersonated Credentials**:
   - **Import Modules**: Imports necessary libraries for authentication and BigQuery operations.
     - `import google.auth`
     - `import google.auth.impersonated_credentials`
     - `from etlQAPackage import iterativeDuplicateCheck`
   - **Define Scopes**: Sets `target_scopes` to `['https://www.googleapis.com/auth/cloud-platform']`.
   - **Obtain Default Credentials**: Calls `google.auth.default()` to get `source_credentials` and `project_id`.
   - **Create Impersonated Credentials**: Uses `google.auth.impersonated_credentials.Credentials` with:
     - `source_credentials`
     - `target_principal` set to `IMPERSONATION_CHAIN`
     - `target_scopes`

2. **Execute Duplication Check**:
   - **Call DuplicateCheck Function**:
     - Executes `iterativeDuplicateCheck.DuplicateCheck()` with the provided parameters and `credentials=tcreds`.
     - **Parameters Passed**:
       - `work_table`
       - `job_id`
       - `gcp_project`
       - `label_key`
       - `label_value`
       - `input_columns`
       - `threshold`
       - `json_config`
       - `credentials`

3. **Integrate with Airflow DAG**:
   - **Create Operator**:
     - Uses `PythonVirtualenvOperator` to create a task named `'duplication_check_' + str(table_name)`.
   - **Configure Operator**:
     - **python_callable**: Sets to `venv_duplication_check`.
     - **python_version**: Specifies `'3.8'`.
     - **requirements**: Includes necessary frameworks via `FRAMEWORKS_VERSION`.
     - **system_site_packages**: Sets `system_site_packages=True` to include system packages.
     - **op_kwargs**: Passes all required parameters, including dynamic values from Airflow's XCom and environment variables.
   - **Add to DAG**: Attaches the operator to the Airflow DAG for execution.

4. **Error Handling**:
   - **Assumption of Success**: The function assumes successful execution; any exceptions will be managed by Airflow's task failure mechanisms.
   - **No Explicit Error Handling**: Does not include try-except blocks within the function.

5. **Edge Cases**:
   - **Invalid Credentials**: If impersonated credentials cannot be obtained, the function will fail when attempting to execute the duplication check.
   - **Invalid Table or Columns**: Does not handle cases where `work_table` does not exist or `input_columns` are invalid.
   - **Threshold Not Met**: If duplicates exceed the threshold, the `DuplicateCheck` function is expected to raise an exception.

---

**Note**: This function is essential for maintaining data quality within data pipelines by identifying and handling duplicate records in BigQuery tables. By utilizing impersonated credentials and integrating with Airflow, it ensures secure and efficient data processing within the Google Cloud environment.

---
# Null Check Function

## Overview

The `venv_null_check` function performs a null value check on specified columns of a BigQuery table within an Airflow DAG. It utilizes impersonated credentials for secure access to Google Cloud resources and integrates with Airflow using the `PythonVirtualenvOperator`.

## Purpose

- **Data Quality Assurance**: Ensures data integrity by identifying null values in specified columns of a BigQuery table.
- **Automated Pipeline Integration**: Seamlessly incorporates null checks into data processing pipelines within Airflow.
- **Secure Authentication**: Uses impersonated credentials to securely interact with Google Cloud services without exposing sensitive credentials.

## Parameters

- **work_table** *(str)*: The BigQuery table on which the null check will be performed.
  - **Optional**: No
- **job_id** *(str)*: A unique identifier for the null check job.
  - **Optional**: No
- **label_key** *(str)*: A key for labeling the BigQuery job.
  - **Optional**: No
- **label_value** *(str)*: A value associated with the label key for job tracking.
  - **Optional**: No
- **INPUT_COLUMNS** *(list)*: A list of column names to check for null values.
  - **Optional**: No
- **threshold** *(dict)*: A dictionary specifying acceptable thresholds for null values per column.
  - **Optional**: No
- **gcp_project** *(str)*: The Google Cloud project ID where the BigQuery table resides.
  - **Optional**: No
- **json_config** *(dict)*: JSON configuration details for auditing or logging purposes.
  - **Optional**: No
- **airflow_conn_id** *(str)*: The Airflow connection ID for retrieving credentials.
  - **Optional**: Yes
  - **Default Value**: `''` (empty string)
- **IMPERSONATION_CHAIN** *(str)*: The service account email used for impersonation to authenticate BigQuery access.
  - **Optional**: No

### Internal Variables

- **target_scopes** *(list)*: The OAuth scopes required for the impersonated credentials (`['https://www.googleapis.com/auth/cloud-platform']`).
- **source_credentials** *(Credentials)*: The source credentials obtained from the default environment.
- **tcreds** *(Credentials)*: The impersonated credentials used for authentication.

## Returns

- **None**: The function does not return any value; it performs the null check operation.
- **Side Effects**: Executes a null check on BigQuery, potentially raising exceptions if null values exceed the specified thresholds.

## Authentication

- **Services Used**:
  - **Google Cloud BigQuery**: Executes SQL queries on BigQuery tables.
  - **Google Authentication Library**: Manages credential creation and impersonation.
- **Authentication Methods**:
  - **Impersonated Credentials**: Utilizes `google.auth.impersonated_credentials.Credentials` to impersonate the target service account specified by `IMPERSONATION_CHAIN`.
- **Permissions Required**:
  - The source service account must have the `roles/iam.serviceAccountTokenCreator` role on the target service account.
  - The target service account must have appropriate permissions to access BigQuery resources.

## Functionality

1. **Step-by-Step Explanation**:

   - **Import Necessary Modules**:
     - Imports `NullCheck` from `etlQAPackage.nullCheck`.
     - Imports authentication modules from `google.auth`.
   - **Set Up Impersonated Credentials**:
     - Defines `target_scopes` for OAuth permissions.
     - Obtains `source_credentials` and `project_id` using `google.auth.default()`.
     - Creates impersonated credentials (`tcreds`) targeting `IMPERSONATION_CHAIN` with `target_scopes`.
   - **Execute Null Check**:
     - Calls `nullCheck.NullCheck` with the provided parameters and `credentials=tcreds`.
     - The function checks each column in `INPUT_COLUMNS` for null values against the specified `threshold`.

2. **Error Handling**:

   - **Exception Raising**: If the null value count exceeds the specified threshold for any column, `NullCheck` is expected to raise an exception.
   - **Credential Errors**: If impersonated credentials cannot be obtained, an authentication error will be raised.
   - **Logging**: Errors are typically logged for debugging and auditing purposes.

3. **Edge Cases**:

   - **Invalid Table or Columns**: If `work_table` does not exist or columns in `INPUT_COLUMNS` are invalid, the function will raise an error.
   - **Empty Input**: If `INPUT_COLUMNS` is empty, the function may either perform no action or raise an exception, depending on the implementation of `NullCheck`.
   - **Threshold Values**: If thresholds are not properly defined, the function may not behave as expected.

---

**Note**: This function is crucial for maintaining data quality within data pipelines by identifying and handling null values in BigQuery tables. By integrating with Airflow and utilizing impersonated credentials, it ensures secure and efficient data processing within the Google Cloud environment.




