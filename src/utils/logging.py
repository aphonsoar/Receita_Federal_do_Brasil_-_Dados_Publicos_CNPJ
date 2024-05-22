import logging

# Configure logging
logging.basicConfig(
    filename="logs/logfile.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",  # Append to existing log file
)

# Use the logger
logger = logging.getLogger(__name__)

logger.info("Logging started.")