#!/bin/bash
set -e

# Warten, bis die PostgreSQL-Datenbank verfügbar ist
echo "Warte auf PostgreSQL-Datenbank..." 

psql -v ON_ERROR_STOP=1 --username user << EOSQL

        CREATE USER user WITH PASSWORD 'password';
        CREATE DATABASE my_database;
        GRANT ALL PRIVILEGES ON DATABASE my_database TO user;

EOSQL

psql -v ON_ERROR_STOP=1 --username user \
  --dbname=my_database --command "GRANT ALL ON SCHEMA public TO user"

psql -v ON_ERROR_STOP=1 --username user --dbname=my_database \
  --file /home/SQLData/tableSchema.sql

psql -v ON_ERROR_STOP=1 --username user --dbname=my_database \
  --file /home/SQLData/insertData.sql