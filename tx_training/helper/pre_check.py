import boto3
import json
import os
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

# Fetch values from widgets (Databricks task parameters)
job_name = dbutils.jobs.taskValues.get(taskKey="create_params", key="job_name")
data_date = dbutils.jobs.taskValues.get(
    taskKey="create_params", key="data_date")
skip_condition = dbutils.jobs.taskValues.get(
    taskKey="create_params", key="skip_condition")

logger.info(
    f"Job Parameters - Job Name: {job_name}, Data Date: {data_date}, Skip Condition: {skip_condition}")

metadata_filepath = f"/Workspace/Shared/tx_project_metadata/{job_name}.json"


def get_depend_jobs(metadata_filepath):
    """Retrieve dependent jobs from metadata."""
    with open(metadata_filepath, "r") as f:
        meta_info = json.load(f)
    depend_jobs = meta_info.get("depend_jobs", [])
    logger.info(f"Dependent jobs to check: {depend_jobs}")
    return depend_jobs


def check_job_status(job_name, run_date):
    """Check the execution status of a dependent job."""
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1')
    table = dynamodb.Table('tx_log')

    response = table.get_item(
        Key={
            'job_name': job_name,
            'data_date': run_date
        }
    )
    if 'Item' not in response:
        raise ValueError(
            f"Job {job_name} with run date {run_date} does not exist in the log table!")

    job_info = response['Item']
    status = job_info.get('job_state', '').lower()

    if status != "success":
        logger.error(f"Job {job_name} is not yet completed successfully.")
        return False

    return True


def check_input_data(metadata_filepath):
    """Verify that all input files are available before proceeding."""
    with open(metadata_filepath, "r") as f:
        meta_info = json.load(f)
    input_files = [entry["table"] for entry in meta_info.get("input", [])]

    missing_files = [file for file in input_files if not os.path.exists(file)]
    if missing_files:
        raise ValueError(
            f"The following input files are missing: {missing_files}")

    logger.info("All input files are available.")


def main():
    """Main function to execute pre-check validations."""
    if skip_condition.lower() == "true":
        logger.info("Skipping pre-check as per condition.")
        return

    depend_jobs = get_depend_jobs(metadata_filepath)

    for job in depend_jobs:
        if not check_job_status(job, data_date):
            raise ValueError(
                f"Dependent jobs {depend_jobs} are not yet completed successfully.")

    check_input_data(metadata_filepath)
    logger.info("Pre-check validation completed successfully.")


if __name__ == "__main__":
    main()
