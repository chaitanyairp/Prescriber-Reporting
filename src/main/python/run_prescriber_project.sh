#!/usr/bin/bash

# Declare a variable to hold Job name
JOBNAME="run_prescriber_project"

# Declare a variable to hold current timestamp
data=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a log file to write output of this job to log_file
LOGFILE="/home/azureuser/projects/PrescriberReporting/src/main/logs/${JOBNAME}_${date}.log"


##########################################################################################
## All the statements below will log output/error to logfile
##########################################################################################
{ # --------------> Start of job <----------------->
  echo "${JOBNAME} started at ${date}.."
  SCRIPTS_PATH="/home/azureuser/projects/PrescriberReporting/src/main/python/"

  # Run script to move files from gateway node to hdfs.
  echo "Calling copy_input_files_to_hdfs.sh script."
  ${SCRIPTS_PATH}/copy_input_files_to_hdfs.sh
  echo "Completed calling copy_input_files_to_hdfs.sh script."

  # Run script to delete output files in hdfs
  echo "Calling delete_hdfs_output_paths.sh script."
  ${SCRIPTS_PATH}/delete_hdfs_output_paths.sh
  echo "Completed delete_hdfs_output_paths.sh script."

  # Run your spark-submit command
  echo "Running spark-submit."
  spark-submit --master yarn ${SCRIPTS_PATH}/run_prescriber_pipeline.py
  echo "Completed spark-submit"

  # Run script to copy files to local
  echo "Calling copy_output_to_local.sh script."
  #${SCRIPTS_PATH}/copy_output_to_local.sh
  echo "Completed copy_output_to_local.sh script."

  # Run script to copy files to azure blob
  echo "Calling copy_to_azure_blob.sh script."
  #${SCRIPTS_PATH}/copy_to_azure_blob.sh
  echo "Completed copy_to_azure_blob.sh script."

  echo "${JOBNAME} ended at ${date}.."
} > "${LOGFILE}" 2>&1
# --------------> End of job <-------------->