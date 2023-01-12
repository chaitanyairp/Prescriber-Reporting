#!/usr/bin/bash

# Declare a variable to hold Job name
JOBNAME="copy_output_to_local"

# Declare a variable to hold current timestamp
data=$(date '+%Y-%m-%d_%H:%M:%S')

# Define a log file to write output of this job to log_file
LOGFILE="/home/azureuser/projects/PrescriberReporting/src/main/logs/${JOBNAME}_${date}.log"

##########################################################################################
## All the statements below will log output/error to logfile
##########################################################################################
{ # --------------> Start of job <----------------->
  echo "${JOBNAME} started at ${date}.."

  LOCAL_OUTPUT_PATH="/home/azureuser/projects/PrescriberReporting/src/main/output"
  LOCAL_CITY_PATH="${LOCAL_OUTPUT_PATH}/city"
  LOCAL_FACT_PATH="${LOCAL_OUTPUT_PATH}/fact"

  HDFS_OUTPUT_PATH="PrescriberReports/reports"
  HDFS_CITY_PATH="${HDFS_OUTPUT_PATH}/city"
  HDFS_FACT_PATH="${HDFS_OUTPUT_PATH}/fact"

  hadoop fs -get $HDFS_CITY_PATH/* $LOCAL_CITY_PATH/
  hadoop fs -get $HDFS_FACT_PATH/* $LOCAL_FACT_PATH/

  echo "${JOBNAME} ended at ${date}.."
} > "${LOGFILE}" 2>&1
# --------------> End of job <-------------->