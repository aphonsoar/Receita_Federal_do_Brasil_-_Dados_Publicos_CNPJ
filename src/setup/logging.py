import logging
import sys

from datetime import datetime
from dotenv import load_dotenv
from os import getenv, makedirs, path

from utils.logging import clear_latest_items

# Configure logging with a single handler
# Set the overall logging level
load_dotenv()
environment = getenv('ENVIRONMENT', 'development')

if environment == 'development':
    logging.basicConfig(
        level=logging.INFO,  
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    
    logging.basicConfig(
        level=logging.WARN,  
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stderr),
        ]
    )

# Create separate log files for errors and info
date_str = datetime.now().strftime("%Y-%m-%d")
time_str = datetime.now().strftime("%H_%M")

log_root_path = f'logs/{date_str}'

# Clear the latest 5 files (adjust 'n' as needed)
if path.exists(log_root_path):
    clear_latest_items(log_root_path, 5)

base_path = f"{log_root_path}/{time_str}"
error_file = f"{base_path}/error_log.log"
info_file = f"{base_path}/info_log.log"

error_path = path.dirname(error_file)
info_path = path.dirname(info_file)

# Create the directory structure if it doesn't exist
makedirs(info_path, exist_ok=True)
makedirs(error_path, exist_ok=True)

error_handler = logging.FileHandler(error_file, mode="a")
error_handler.setLevel(logging.ERROR)  # Only log errors to this file

info_handler = logging.FileHandler(info_file, mode="a")
info_handler.setLevel(logging.INFO)  # log INFO and above to this file

warn_handler = logging.FileHandler(info_file, mode="a")
warn_handler.setLevel(logging.WARN)  # Only log INFO and above to this file

logging.getLogger().addHandler(error_handler)
logging.getLogger().addHandler(info_handler)
logging.getLogger().addHandler(warn_handler)

# Use the logger
logger = logging.getLogger(__name__)

logger.info("Logging started.")


