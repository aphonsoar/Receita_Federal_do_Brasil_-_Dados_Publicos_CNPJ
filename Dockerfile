FROM python:3.9-slim-bullseye

WORKDIR /app

# Copy your application code
COPY src/ ./src
COPY .env .

# Create the log directory
RUN mkdir -p /app/logs

# Install the required packages
RUN apt-get update && apt-get -y install cron python3 python3-pip virtualenv

# RUN python3 -m venv /opt/venv

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the crontab file to the cron.d directory
COPY cron-config /etc/cron.d/cron-config

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/cron-config

# Apply cron job
RUN crontab /etc/cron.d/cron-config

# Create the log file to be able to run tail
RUN touch /app/logs/cron.log

# Run the command on container startup
CMD echo "starting" && echo "continuing" && (cron) \
&& echo "tailing..." && : >> /app/logs/cron.log && tail -f /app/logs/cron.log
