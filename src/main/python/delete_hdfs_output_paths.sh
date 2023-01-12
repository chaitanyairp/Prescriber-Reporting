#!/usr/bin/bash

# Declare a variable to hold job name
JOBNAME="delete_hdfs_output_paths"

# Declare a variable to hold current timestamp
date=$(date '+%Y-%m-%d_%H:%M:%S')

# Define path to store the logs of the script
LOGFILE="/home/azureuser/projects/PrescriberReporting/src/main/logs/${JOBNAME}_${date}.log"

##########################################################################################
## All the statements below will log output/error to logfile
##########################################################################################
{ # --------------> Start of job <----------------->
  echo "${JOBNAME} started at ${date}.."

  CITY_PATH=PrescriberReports/reports/city
  FACT_PATH=PrescriberReports/reports/fact

  hdfs dfs -test -d ${CITY_PATH}
  status=$?

  if [ $status == 0 ]
    then
    echo "The HDFS output dir ${CITY_PATH} is available. Continue to delete the directory."
    hdfs dfs -rm -R -f ${CITY_PATH}
    echo "The HDFS output dir ${CITY_PATH} is deleted."
  fi

  hdfs dfs -test -d ${FACT_PATH}
  status=$?

  if [ $status == 0 ]
    then
    echo "The HDFS output dir ${FACT_PATH} is available. Continue to delete the directory."
    hdfs dfs -rm -R -f ${CITY_PATH}
    echo "The HDFS output dir ${FACT_PATH} is deleted."
  fi

  echo "${JOBNAME} ended at ${date}"
} > "${LOGFILE}" 2>&1
# --------------> End of job <-------------->
