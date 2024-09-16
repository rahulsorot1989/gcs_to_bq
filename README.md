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
