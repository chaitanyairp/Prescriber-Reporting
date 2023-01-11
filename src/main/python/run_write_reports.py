import variables as gav
import logging
import logging.config

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