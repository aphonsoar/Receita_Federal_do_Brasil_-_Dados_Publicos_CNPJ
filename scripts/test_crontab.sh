#!/bin/bash

# Read ENVIRONMENT variable from .env file (assuming it's at the root)
source "$1/.env"  # Load environment variables
echo $1

# Set default frequency (midnight)
frequency="0 0 * * *"

# Check if ENVIRONMENT is set to "development"
if [[ "$ENVIRONMENT" == "development" ]]; then
    # Update frequency for every 5 minutes
    frequency="*/5 * * * *"
fi

# Source path and log file (assuming paths remain the same)
root_path="/root/github/RF_CNPJ"
source_path="$root_path/src/main.py"
log_file="$root_path/logs/crontab_logs.log"

# Prepare the cron entry
cron_entry="/usr/bin/python3 $source_path >> $log_file 2>&1"

# Update crontab with the appropriate frequency
crontab -l | grep -v "$cron_entry" | crontab -  # Remove any existing entry (avoid duplicates)
echo "$frequency $cron_entry" | crontab -  # Add the new entry with chosen frequency

echo "Scheduled script to run with frequency: $frequency"
