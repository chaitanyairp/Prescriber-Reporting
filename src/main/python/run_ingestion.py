from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import logging
import logging.config

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def read_city_file(spark, file_path_with_name):
    try:
        logger.info("Starting read_city_file().")
        df = spark.read\
            .format("parquet")\
            .option("path", file_path_with_name)\
            .load()

        logger.info("Completed read_city_file().")
    except Exception as exp:
        logger.error("Error in read_city_file().", exc_info=True)
        raise
    else:
        return df


def read_fact_file(spark, file_path_with_name):
    try:
        logger.info("Starting read_fact_file().")
        fact_schema = StructType([
            StructField("npi", IntegerType()),
            StructField("nppes_provider_last_org_name", StringType()),
            StructField("nppes_provider_first_name", StringType()),
            StructField("nppes_provider_city", StringType()),
            StructField("nppes_provider_state", StringType()),
            StructField("specialty_description", StringType()),
            StructField("description_flag", StringType()),
            StructField("drug_name", StringType()),
            StructField("generic_name", StringType()),
            StructField("bene_count", StringType()),
            StructField("total_claim_count", StringType()),
            StructField("total_30_day_fill_count", StringType()),
            StructField("total_day_supply", StringType()),
            StructField("total_drug_cost", StringType()),
            StructField("bene_count_ge65", StringType()),
            StructField("bene_count_ge65_suppress_flag", StringType()),
            StructField("total_claim_count_ge65", StringType()),
            StructField("ge65_suppress_flag", StringType()),
            StructField("total_30_day_fill_count_ge65", StringType()),
            StructField("total_day_supply_ge65", StringType()),
            StructField("total_drug_cost_ge65", StringType()),
            StructField("years_of_exp", StringType())
        ])

        df = spark.read\
            .format("csv")\
            .option("header", True)\
            .schema(fact_schema)\
            .option("path", file_path_with_name)\
            .load()
        logger.info("Completed read_fact_file().")
    except Exception as exp:
        logger.error("Error in read_fact_file().", exc_info=True)
        raise
    else:
        return df



