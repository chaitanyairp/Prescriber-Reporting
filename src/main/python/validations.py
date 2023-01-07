import logging
import logging.config

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def validate_spark_object(spark):
    # Comments: Need to see how to format the list of timestamp that we get.
    logger.info("Starting validate_spark_object()")
    dt = spark.sql("select current_timestamp").collect()
    logger.info(dt)


def print_schema_of_df(df, df_name):
    try:
        logger.info(f"Starting print_schema for dataframe {df_name}")
        for field in df.schema:
            logger.info(field + "\\n")
        logger.info(f"Completed print_schema for dataframe {df_name}")
    except Exception as exp:
        logger.error("Error in func print_schema_of_df", exc_info=True)
        raise


def print_top_ten_rows(df, df_name):
    try:
        logger.info(f"Started print_top_ten_rows for dataframe {df_name}")
        #df.show(10, truncate=False)
        # How to print rows to logger files??????????
        logger.info(f"Completed print_top_ten_rows for dataframe {df_name}")
    except Exception as exp:
        logger.error(f"Error in print_top_ten_rows for dataframe {df_name}", exc_info=True)
        raise



