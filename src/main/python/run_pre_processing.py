import logging
import logging.config
import variables as gav

from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

logging.config.fileConfig(fname=gav.log_conf_file_path)
logger = logging.getLogger(__name__)


def run_data_cleaning_city(df, df_name):
    try:
        logger.info(f"Started data cleaning for dataframe {df_name}")

        city_sel_df = df.select(
            f.upper(f.col("city")).alias("city"),
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
        return city_sel_df


def run_data_cleaning_fact(df, df_name):
    try:
        logger.info("Starting run_data_cleaning_fact() func.")
        fact_sel_df = df.select(
            f.col("npi").alias("provider_id"),
            f.col("nppes_provider_last_org_name").alias("provider_lname"),
            f.col("nppes_provider_first_name").alias("provider_fname"),
            f.col("nppes_provider_city").alias("provider_city"),
            f.col("nppes_provider_state").alias("provider_state"),
            f.lit("USA").alias("country"),
            f.col("total_claim_count").cast("int").alias("claim_count"),
            f.col("years_of_exp")
        )

        fact_df = fact_sel_df.withColumn("provider_name", f.concat_ws(" ", f.col("provider_fname"), f.col("provider_lname")))\
            .drop("provider_fname", "provider_lname")

        fact_df = fact_df.withColumn("years_of_exp", f.regexp_extract("years_of_exp", r"\d+", 0).cast("int"))

        fact_df = fact_df.dropna(subset=["provider_id"])

        fact_df = fact_df.withColumn("avg_claim_count", f.avg("claim_count").over(w.partitionBy("provider_id")))

        fact_df = fact_df.withColumn("claim_count", f.coalesce(f.col("claim_count"), f.col("avg_claim_count")))\
                    .drop("avg_claim_count")

        # fact_df.select(
        #      [f.count(f.when(f.col(c).isNull() | f.isnan(c), c)).alias(c) for c in fact_df.columns]
        # ).show()

        logger.info("Completed executing run_data_cleaning_fact() func.")
    except Exception as exp:
        logger.error("Error in run_data_cleaning_fact() for fact_df", exc_info=True)
        raise
    else:
        logger.info("Completed cleaning of fact file")
        return fact_df
