#!/usr/bin/bash

# Declare a variable to hold Job name
JOBNAME="copy_to_azure_blob"

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

  # How to generate URLS? Go to containers, access policy, sas tokens - generate take url
  city_sas_url="https://saprescriberanalytics.blob.core.windows.net/city?st=2023-01-12T19:07:59Z&se=2023-01-13T03:07:59Z&si=READ_WRITE&spr=https&sv=2021-06-08&sr=c&sig=z5MOvt%2BX8Yih%2FLzaXQjy%2FPdcXuWNNac%2F7TcqU%2BvM%2ByM%3D"
  fact_sas_url="https://saprescriberanalytics.blob.core.windows.net/fact?st=2023-01-12T19:06:16Z&se=2023-01-13T03:06:16Z&si=READ_WRITE&spr=https&sv=2021-06-08&sr=c&sig=mPQybxTC8CQEUZzK2teqhxalPSHa0%2BU5RYUpiUL5vTo%3D"

  azcopy copy "${LOCAL_CITY_PATH}/*" "${city_sas_url}"
  azcopy copy "${LOCAL_FACT_PATH}/*" "${fact_sas_url}"

  echo "${JOBNAME} ended at ${date}.."
} > "${LOGFILE}" 2>&1
# --------------> End of job <-------------->