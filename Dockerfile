FROM python:3.8-slim

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y cron

# Copy your application code
COPY ./src /app

# Copy the crontab file to the cron.d directory
COPY cron-setup /etc/cron.d/cron-setup

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cron-setup

# Apply cron job
RUN crontab /etc/cron.d/cron-setup

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the command on container startup
CMD cron && tail -f /var/log/cron.log