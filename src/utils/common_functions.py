import logging
import os

def setup_logging(level=logging.DEBUG):
    """
    Set up logging with the specified log level.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s"
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    return logger

logger = setup_logging()

def create_directory(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info("Directory '%s' created.", directory_path)
    else:
        logger.info("Directory '%s' already exists.", directory_path)