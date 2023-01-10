import os

#Running from Test env
os.environ["env"] = "Test"

# Get variables
env = os.environ["env"]

# File paths for local system
curr_dir = os.getcwd()
city_file_path = curr_dir + "\\..\\staging\\city"
fact_file_path = curr_dir + "\\..\\staging\\fact"

city_file_path_hdfs = ""
fact_file_path_hdfs = ""


log_conf_file_path = r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf"
log_conf_file_path_hdfs = r""


