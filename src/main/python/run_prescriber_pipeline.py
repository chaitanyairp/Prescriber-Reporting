from create_objects import get_or_create_spark_session
from validations import validate_spark_object

import logging
import logging.config

logging.config.fileConfig(fname=r"C:\Users\chait\PycharmProjects\Prescriber Reporting\src\main\config\log_to_file.conf")
logger = logging.getLogger(__name__)


def main():
    try:
        logger.info("Starting main() of run_prescriber_pipeline.")
        # Create a spark session
        spark = get_or_create_spark_session()
        # Validate spark session created.
        validate_spark_object(spark)

        logger.info("Ending main() of run_prescriber_pipeline.")
    except Exception as exp:
        logger.error("Error in main() of run_prescriber_pipeline", exc_info=True)


if __name__ == "__main__":
    main()