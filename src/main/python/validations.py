import logging
import logging.config

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def validate_spark_object(spark):
    # Comments: Need to see how to format the list of timestamp that we get.
    logger.info("Starting validate_spark_object()")
    dt = spark.sql("select current_timestamp").collect()
    logger.info(dt)

