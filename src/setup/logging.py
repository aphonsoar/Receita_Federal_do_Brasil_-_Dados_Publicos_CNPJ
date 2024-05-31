import logging
import sys

from datetime import datetime
from dotenv import load_dotenv
from os import getenv, makedirs, path
from pythonjsonlogger import jsonlogger

from utils.logging import clear_latest_items

# Use the logger
logger = logging.getLogger(__name__)

# Configure logging with a single handler
# Set the overall logging level
load_dotenv()
ENVIRONMENT = getenv('ENVIRONMENT', 'development')

fields = [
    "threadName",
    "name",
    "thread",
    "created",
    "process",
    "processName",
    "relativeCreated",
    "module",
    "funcName",
    "levelno",
    "msecs",
    "pathname",
    "lineno",
    "asctime",
    "message",
    "filename",
    "levelname",
    "special",
    "run"
]

logging_format=" ".join(map(lambda field_name: f"%({field_name})s", fields))
fmt = jsonlogger.JsonFormatter(logging_format)

if ENVIRONMENT == 'development':
    # Create a handler for stdout and stderr
    stdout_stream_handler=logging.StreamHandler(sys.stdout)
    stderr_stream_handler=logging.StreamHandler(sys.stderr)

    # Set the format for the handlers
    stdout_stream_handler.setFormatter(fmt)
    stderr_stream_handler.setFormatter(fmt)

    logging.basicConfig(
        level=logging.INFO,
        handlers=[stdout_stream_handler]
    )
    
    logging.basicConfig(
        level=logging.WARN,  
        handlers=[stderr_stream_handler]
    )

# Create separate log files for errors and info
date_str = datetime.now().strftime("%Y-%m-%d")
time_str = datetime.now().strftime("%H_%M")

log_root_path = f'logs/{date_str}'

# Clear the latest 5 files (adjust 'n' as needed)
LOG_FILES_HORIZON = int(getenv('LOG_FILES_HORIZON', 5))
if path.exists(log_root_path):
    clear_latest_items(log_root_path, LOG_FILES_HORIZON)

base_path = f"{log_root_path}/{time_str}"
error_file = f"{base_path}/error_log.log"
info_file = f"{base_path}/info_log.log"

error_path = path.dirname(error_file)
info_path = path.dirname(info_file)

# Create the directory structure if it doesn't exist
makedirs(info_path, exist_ok=True)
makedirs(error_path, exist_ok=True)

log_infos = [
    (error_file, logging.ERROR),
    (info_file, logging.INFO),
    (info_file, logging.WARN),
]

for log_file, log_level in log_infos:
    log_handler = logging.FileHandler(log_file, mode="a")
    log_handler.setFormatter(fmt)
    log_handler.setLevel(log_level)  # Only log errors to this file

    logger.addHandler(log_handler)

logger.info("Logging started.")


