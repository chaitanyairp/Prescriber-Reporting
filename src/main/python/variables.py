import os

#Running from Test env
os.environ["env"] = "Test"

# Get variables
env = os.environ["env"]

# File paths for local system
curr_dir = os.getcwd()
city_file_path = curr_dir + "..\\staging\\city"
fact_file_path = curr_dir + "..\\staging\\fact"



