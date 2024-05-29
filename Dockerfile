FROM python:3.9-slim-bullseye

WORKDIR /app

# Copy your application code
COPY src/ ./src
COPY .env .

# Install the required packages
RUN apt-get update && apt-get -y install cron python3 python3-pip postgresql-client

# Initialize the database
COPY scripts/init_db.sh /app
COPY scripts/init_db.sql /app
RUN chmod +x /app/init_db.sh
RUN /app/init_db.sh

# Create the log directory
RUN mkdir -p /app/logs

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

# Cron jobs
RUN echo '* * * * * pip install -r requirements.txt && python3 /app/src/main.py >> /app/logs/cron.log 2>&1' > cron-config

# Apply cron job
RUN crontab cron-config

# Create the log file to be able to run tail
RUN touch /app/logs/cron.log

# Run the command on container startup
CMD echo "starting" && echo "continuing" && (cron) \
&& echo "tailing..." && : >> /app/logs/cron.log && tail -f /app/logs/cron.log
