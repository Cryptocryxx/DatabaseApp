#!/bin/bash
set -e

# Warten, bis die PostgreSQL-Datenbank verfügbar ist
echo "Warte auf PostgreSQL-Datenbank..."
until pg_isready -h localhost -p 5432 -U "$POSTGRES_USER"; do
  sleep 1
done

# SQL-Datei ausführen, falls vorhanden
if [ -f /docker-entrypoint-initdb.d/testdatabase.sql ]; then
  echo "Starte die Datenbankinitialisierung..."
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/testdatabase.sql
else
  echo "SQL-Datei nicht gefunden."
fi

# PostgreSQL-Prozess starten
exec postgres