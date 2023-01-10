import logging
import logging.config
import pandas
import variables as gav

logging.config.fileConfig(fname=gav.log_conf_file_path)
logger = logging.getLogger(__name__)


def validate_spark_object(spark):
    # Comments: Need to see how to format the list of timestamp that we get.
    logger.info("Starting validate_spark_object()")
    dt = spark.sql("select current_timestamp").collect()
    logger.info(str(dt))


def print_schema_of_df(df, df_name):
    try:
        logger.info(f"Starting print_schema for dataframe {df_name}")
        for field in df.schema:
            logger.info(f"\t{field}")
        logger.info(f"Completed print_schema for dataframe {df_name}")
    except Exception as exp:
        logger.error("Error in func print_schema_of_df", exc_info=True)
        raise


def print_top_ten_rows(df, df_name):
    try:
        logger.info(f"Started print_top_ten_rows for dataframe {df_name}")

        top_rec = df.limit(10)
        df_pandas = top_rec.toPandas()
        logger.info("\n \t" + df_pandas.to_string(index=False))

        logger.info(f"Completed print_top_ten_rows for dataframe {df_name}")
    except Exception as exp:
        logger.error(f"Error in print_top_ten_rows for dataframe {df_name}", exc_info=True)
        raise


def get_count_of_df(df, df_name):
    try:
        logger.info(f"Started get_count_of_df for dataframe {df_name}")
        cnt = df.count()
        logger.info(f"Number of records in df {df_name} are {cnt}")
        logger.info(f"Completed get_count_of_df for dataframe {df_name}")
    except Exception as exp:
        logger.error(f"Error in get_count_of_df for dataframe {df_name}", exc_info=True)
        raise

