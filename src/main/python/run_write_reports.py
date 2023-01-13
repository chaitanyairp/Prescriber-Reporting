import variables as gav
import logging
import logging.config
from pyspark.sql.functions import current_date

logging.config.fileConfig(fname=gav.log_conf_file_path)
logger = logging.getLogger(__name__)


def write_reports_to_hdfs(df, df_name, file_format, file_path, split_count=None, compression_type=None):
    try:
        if split_count:
            df = df.repartition(split_count)

        logger.info(f"Started write_report_to_hdfs for dataframe {df_name}")

        if compression_type:
            df.write\
                .format(file_format)\
                .option("path", file_path)\
                .option("compression", compression_type)\
                .save()
        else:
            df.write \
                .format(file_format) \
                .option("path", file_path) \
                .save()

    except Exception as exp:
        logger.error("Error in write_report_to_hdfs for dataframe {df_name}", exc_info=True)
        raise
    else:
        logger.info(f"Completed write_report_to_hdfs for dataframe {df_name}")


def write_reports_to_hive(spark, city_df, fact_df):
    try:
        logger.info("Started write_reports_to_hive()..")
        spark.sql("""create database if not exists prescriber""")

        # Write city report to hive city_report table
        city_df = city_df.withColumn("kots", current_date())
        city_df.write\
            .format("orc") \
            .partitionBy("kots")\
            .mode("append")\
            .option("path", "/PrescriberReports/hive/external/warehouse/prescriber.db")\
            .saveAsTable("prescriber.city_report")

        # Write fact report to hive fact_report table
        fact_df = fact_df.withColumn("kots", current_date())
        fact_df.write\
            .format("orc") \
            .mode("append") \
            .partitionBy("kots") \
            .option("path", "/PrescriberReports/hive/external/warehouse/prescriber.db") \
            .saveAsTable("prescriber.fact_report")
        # In case of external hive tables, we can specify path. Above is external table.

        logger.info("Completed write_reports_to_hive()..")
    except Exception as exp:
        logger.error("Error in write_reports_to_hive()..", exc_info=True)
        raise
    else:
        logger.info("Done with write_reports_to_hive()..")



