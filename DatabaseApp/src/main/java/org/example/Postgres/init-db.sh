#!/bin/bash
set -e

# Warten, bis die PostgreSQL-Datenbank verfügbar ist
echo "Warte auf PostgreSQL-Datenbank..." 

psql -v ON_ERROR_STOP=1 --username "user" --dbname="my_database" << EOSQL

EOSQL


psql -v ON_ERROR_STOP=1 --username user --dbname=my_database \
  --file /home/SQLData/tableSchema.sql

  psql -v ON_ERROR_STOP=1 --username user --dbname=my_database \
    --file /home/SQLData/insertData.sql
