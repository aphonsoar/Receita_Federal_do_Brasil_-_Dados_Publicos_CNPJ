#!/bin/bash

source /app/.env

while ! nc -z crawler-db $POSTGRES_PORT; do
  echo "Waiting for postgres..."
  sleep 2
done;

echo "Postgres is up!"

# Execute init_db.sh
/app/init_db.sh

# Replace the following line with your actual application startup command
exec "$@"