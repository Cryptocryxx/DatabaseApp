FROM postgres:13

# Kopiere die SQL-Datei und das Shell-Skript in das Image
COPY testdatabase.sql /docker-entrypoint-initdb.d/
COPY init-db.sh /docker-entrypoint-initdb.d/

# Setze Berechtigungen für das Shell-Skript
RUN chmod +x /docker-entrypoint-initdb.d/init-db.sh

# Setze das Shell-Skript als Entry Point
ENTRYPOINT ["/docker-entrypoint-initdb.d/init-db.sh"]

# Standardmäßig PostgreSQL starten
CMD ["postgres"]