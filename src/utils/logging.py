from loguru import logger

# Configure logger to log messages to a file
logger.add("logs/logfile.log", rotation="500 MB", retention="10 days", compression="zip")

