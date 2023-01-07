from pyspark.sql import SparkSession
from pyspark import SparkConf

import logging
import logging.config

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def get_or_create_spark_session():
    try:
        logger.info("Starting func get_or_create_spark_session().")
        spark_conf = SparkConf()
        spark_conf.set("spark.app.name", "Prescriber Reporting")
        spark_conf.set("spark.master", "local[*]")

        spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
        logger.info("Spark session is created.")
    except Exception as exp:
        logger.error("Exception in func get_or_create_spark_session()", exc_info=True)
        raise
    else:
        logger.info("Spark session is returned.")
        return spark