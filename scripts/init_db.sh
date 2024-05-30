#/bin/bash 

source .env

# Create the database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_NAME" <<-EOSQL
    CREATE DATABASE "$POSTGRES_NAME"
        WITH
        OWNER = "$POSTGRES_USER"
        ENCODING = 'UTF8'
        CONNECTION LIMIT = -1;
    
    CREATE DATABASE "$POSTGRES_NAME"_Test
        WITH
        OWNER = "$POSTGRES_USER"
        ENCODING = 'UTF8'
        CONNECTION LIMIT = -1;

    CREATE USER "$POSTGRES_USER" WITH PASSWORD "$POSTGRES_PASSWORD";
    GRANT pg_read_all_data, pg_write_all_data ON DATABASE "$POSTGRES_NAME"_Test TO "$POSTGRES_USER";
EOSQL