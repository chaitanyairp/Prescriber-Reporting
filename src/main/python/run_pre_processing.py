import logging
import logging.config

from pyspark.sql import functions as f

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def run_data_cleaning_city(df, df_name):
    try:
        logger.info(f"Started data cleaning for dataframe {df_name}")

        city_sel_df = df.select(
            f.upper("city").alias("city"),
            f.col("state_id").alias("state"),
            f.col("county_name").alias("county_name"),
            f.col("population").alias("population"),
            f.col("zips").alias("zips"))

        # Here we will do filter, removing duplicates, casting, select,
        # checking for nulls and removing them, formatting of fields



        logger.info(f"Completed data cleaning for dataframe {df_name}")
    except Exception as exp:
        logger.error(f"Error in data cleaning for dataframe {df_name}", exc_info=True)
    else:
        return df

