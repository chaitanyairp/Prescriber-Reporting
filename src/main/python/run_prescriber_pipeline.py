import os
import sys
from subprocess import Popen, PIPE

from create_objects import get_or_create_spark_session
from validations import validate_spark_object, print_schema_of_df, print_top_ten_rows, get_count_of_df
from run_ingestion import read_city_file, read_fact_file
from run_pre_processing import run_data_cleaning_city, run_data_cleaning_fact
from run_transformations import generate_city_report, generate_fact_report
from run_write_reports import write_reports_to_hdfs, write_reports_to_hive, write_reports_to_rdbms
import variables as gav

import logging
import logging.config

logging.config.fileConfig(fname=gav.log_conf_file_path)
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Starting main() of run_prescriber_pipeline.")
        # Create a spark session
        spark = get_or_create_spark_session()
        # Validate spark session created.
        validate_spark_object(spark)

        # <----------------- Local development ------------------------------------>
        # Get file paths of city and fact files.
        # This code is for local development.
        # city_path = gav.city_file_path
        # city_file_name = os.listdir(city_path)[0]
        # city_path_with_name = city_path + "\\" + city_file_name
        #
        # fact_path = gav.fact_file_path
        # fact_file_name = os.listdir(fact_path)[0]
        # fact_path_with_name = fact_path + "\\" + fact_file_name
        # <-----------------End of local development------------------------------->

        city_path = gav.city_input_file_path
        proc = Popen(["hadoop", "fs", "-ls", "-C", city_path], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if err.decode() != "":
            logger.error(f"No City input file in location {city_path}", exc_info=True)
            raise
        else:
            city_path_with_name = city_path

        fact_path = gav.fact_input_file_path
        proc = Popen(["hadoop", "fs", "-ls", "-C", fact_path], stdout=PIPE, stderr=PIPE)
        (out, err) = proc.communicate()
        if err.decode() != "":
            logger.error(f"No Fact input file in location {fact_path}", exc_info=True)
            raise
        else:
            fact_path_with_name = fact_path

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

        # Run pre-processing for city file
        city_df = run_data_cleaning_city(city_df, "city_df")
        print_schema_of_df(city_df, "city_df")
        print_top_ten_rows(city_df, "city_df")

        # Run pre-processing for fact file
        fact_df = run_data_cleaning_fact(fact_df, "fact_df")
        print_schema_of_df(fact_df, "fact_df")
        print_top_ten_rows(fact_df, "fact_df")

        # Run transformations for city report
        city_report_df = generate_city_report(city_df, fact_df)
        print_schema_of_df(city_report_df, "city_report_df")
        print_top_ten_rows(city_report_df, "city_report_df")

        # Run transformations for fact report
        fact_report_df = generate_fact_report(fact_df)
        print_schema_of_df(fact_report_df, "fact_report_df")
        print_top_ten_rows(fact_report_df, "fact_report_df")

        # Write City report
        write_reports_to_hdfs(city_report_df, "city_report_df", "json", gav.city_output_file_path, 4, "bzip2")

        # Write fact report
        write_reports_to_hdfs(fact_report_df, "fact_report_df", "orc", gav.fact_output_file_path, 1, "snappy")

        # Write city and fact reports to hive
        write_reports_to_hive(spark, city_report_df, fact_report_df)

        # Write reports to postgres
        write_reports_to_rdbms(spark, city_report_df, fact_report_df)

        logger.info("Ending main() of run_prescriber_pipeline.")
    except Exception as exp:
        logger.error("Error in main() of run_prescriber_pipeline", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()