import os

# Running from Test env
os.environ["env"] = "PROD"

# Get variables
env = os.environ["env"]

# File paths for local system
curr_dir = os.getcwd()
# city_file_path = curr_dir + "\\..\\staging\\city"
# fact_file_path = curr_dir + "\\..\\staging\\fact"

# Input File paths for hdfs
prescriber_stage_path = "PrescriberReports/staging"
city_input_file_path = "PrescriberReports/staging/city/"
fact_input_file_path = "PrescriberReports/staging/fact/"

# Output file paths for hdfs
city_output_file_path = "PrescriberReports/reports/city/"
fact_output_file_path = "PrescriberReports/reports/fact/"


# Local env file path
# log_conf_file_path = r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf"
# VM file path
log_conf_file_path = "/home/azureuser/projects/PrescriberReporting/src/main/config/log_to_file.conf"
