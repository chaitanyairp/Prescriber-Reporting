#!/usr/bin/bash

# Declare a variable to hold unix script name
JOBNAME="copy_input_files_to_hdfs"

# Declare date variable to hold current timestamp
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a log file to write output of this job to log_file
LOGFILE="/home/azureuser/projects/PrescriberReporting/src/main/logs/${JOBNAME}_${date}.log"

##########################################################################################
## All the statements below will log output/error to logfile
##########################################################################################
{ # --------------> Start of job <----------------->
  echo "${JOBNAME} started at ${date}.."
  LOCAL_STAGING_PATH="/home/azureuser/files"
  LOCAL_CITY_DIR=${LOCAL_STAGING_PATH}/city
  LOCAL_FACT_DIR=${LOCAL_STAGING_PATH}/fact

  HDFS_STAGING_PATH="PrescriberReports/staging"
  HDFS_CITY_DIR=${HDFS_STAGING_PATH}/city
  HDFS_FACT_DIR=${HDFS_STAGING_PATH}/fact

  hadoop fs -put -f ${LOCAL_CITY_DIR}/* ${HDFS_CITY_DIR}/
  hadoop fs -put -f ${LOCAL_FACT_DIR}/* ${HDFS_FACT_DIR}/

  echo "${JOBANAME} ended at ${date}.."
} > "${LOGFILE}" 2>&1
# --------------> End of job <-------------->