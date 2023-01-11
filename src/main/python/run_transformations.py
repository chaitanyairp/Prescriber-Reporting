import variables as gav
#from udfs import split_count_udf


import logging
import logging.config
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

logging.config.fileConfig(fname=gav.log_conf_file_path)
logger = logging.getLogger(__name__)


def generate_city_report(city_df, fact_df):
    try:
        logger.info("Started generate_city_report().")

        city_df = city_df.withColumn("zip_count", f.size(f.split(f.trim(f.col("zips")), " ")))\
                    .drop("zips")

        # Commenting out as i am not using udf.
        #city_df = city_df.withColumn("zp_cnt", split_count_udf(f.col("zips")))

        fact_df = fact_df.select(
            "provider_city",
            "provider_state",
            "provider_id",
            "country",
            "claim_count"
        )

        fact_df = fact_df.groupBy("provider_city", "provider_state").agg(
            f.first("country").alias("country"),
            f.countDistinct("provider_id").alias("total_prescribers"),
            f.sum("claim_count").alias("total_trx_count")
        )

        city_fact_join_cond = ((f.col("state") == f.col("provider_state")) & (f.col("city") == f.col("provider_city")))
        city_fact_df = city_df.join(fact_df, on=city_fact_join_cond, how="inner").select(
            "city",
            "state",
            "county_name",
            "country",
            "population",
            "zip_count",
            "total_prescribers",
            "total_trx_count"
        )

        logger.info("Completed executing generate_city_report().")
    except Exception as exp:
        logger.error("Error in generate_city_report()", exc_info=True)
        raise
    else:
        return  city_fact_df


def generate_fact_report(fact_df):
    try:
        logger.info("Started generate_fact_report().")

        fact_df  = fact_df.filter((f.col("years_of_exp") >= 20) & (f.col("years_of_exp") <= 50)).select(
            "provider_id",
            "provider_name",
            "provider_city",
            "provider_state",
            "country",
            "claim_count",
            "years_of_exp"
        )

        fact_agg_df = fact_df.groupBy("provider_state", "provider_id").agg(
            f.first("provider_name").alias("provider_name"),
            f.first("provider_city").alias("provider_city"),
            f.first("country").alias("country"),
            f.first("years_of_exp").alias("years_of_exp"),
            f.sum("claim_count").alias("total_trx_count")
        )

        fact_rnk_df = fact_agg_df.withColumn("rnk", f.dense_rank().over(w.partitionBy("provider_state").orderBy(f.col("total_trx_count").desc())))

        fact_report_df = fact_rnk_df.filter("rnk <= 5").drop("rnk")

        logger.info("Completed generate_city_report().")
    except Exception as exp:
        logger.error("Error in generate_fact_report()", exc_info=True)
        raise
    else:
        return fact_report_df

