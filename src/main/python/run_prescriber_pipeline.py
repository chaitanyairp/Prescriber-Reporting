import os

from create_objects import get_or_create_spark_session
from validations import validate_spark_object, print_schema_of_df, print_top_ten_rows, get_count_of_df
from run_ingestion import read_city_file, read_fact_file
import variables as gav

import logging
import logging.config

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Starting main() of run_prescriber_pipeline.")
        # Create a spark session
        spark = get_or_create_spark_session()
        # Validate spark session created.
        validate_spark_object(spark)

        # Get file paths of city and fact files.
        city_path = gav.city_file_path
        city_file_name = os.listdir(city_path)[0]
        city_path_with_name = city_path + "\\" + city_file_name

        fact_path = gav.fact_file_path
        fact_file_name = os.listdir(fact_path)[0]
        fact_path_with_name = fact_path + "\\" + fact_file_name

        # Read city file
        city_df = read_city_file(spark, city_path_with_name)
        print_schema_of_df(city_df, "city_df")
        get_count_of_df(city_df, "city_df")
        print_top_ten_rows(city_df, "city_df")

        # Read fact file
        fact_df = read_fact_file(spark, fact_path_with_name)
        print_schema_of_df(fact_df, "fact_df")
        get_count_of_df(fact_df, "fact_df")
        print_top_ten_rows(fact_df, "fact_df")

        # Run pre processing for city file

        logger.info("Ending main() of run_prescriber_pipeline.")
    except Exception as exp:
        logger.error("Error in main() of run_prescriber_pipeline", exc_info=True)


if __name__ == "__main__":
    main()