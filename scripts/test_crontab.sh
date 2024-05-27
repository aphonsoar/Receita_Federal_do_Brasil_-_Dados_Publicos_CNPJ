#!/bin/bash

# Calculate the time 5 minutes from now
current_minute=$(date +%M)
current_hour=$(date +%H)
new_minute=$(( (10#$current_minute + 5) % 60 ))
new_hour=$(( current_hour + ( (10#$current_minute + 5) / 60 ) ))

root_path = "/root/github/RF_CNPJ"
source_path = f"$root_path/src/main.py"
log_file=f"$root_path/logs/crontab_logs.log"

# Prepare the cron entry
cron_entry="$new_minute $new_hour * * * /usr/bin/python3 $source_path >> $log_file 2>&1"

# Add the entry to the crontab
(crontab -l; echo "$cron_entry") | crontab -

# Print confirmation
echo "Scheduled script to run at $new_hour:$new_minute"