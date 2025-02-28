package org.example.JDBCExporter.Backup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.JDBCExporter.IncrementalExporter.IncrementalHelper;
import org.example.JDBCExporter.IncrementalExporter.IncrementalMain;
import org.example.JDBCExporter.MetaDataController;
import org.example.Logger.Logger;
import org.postgresql.util.PSQLException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RestoreBackup {

    private final Connection connection;
    private final Logger logger;
    private String version;

    public RestoreBackup(Connection connection, Logger logger) {
        this.connection = connection;
        this.logger = logger;
    }

    public void performRestore(String version, String password) throws SQLException {
        logger.info("Verbindung zur Datenbank hergestellt.");
        this.version = version;


        String tableScriptFile = MetaDataController.getInstance().getTableSchemaFilePath(version);
        String constraintsFile = MetaDataController.getInstance().getConstraintsFilePath(version);

        Statement statement = connection.createStatement();
        String[] splitUrl = connection.getMetaData().getURL().split("/");
        int indexOfSlash = connection.getMetaData().getURL().lastIndexOf("/");
        String URL = connection.getMetaData().getURL().substring(0, indexOfSlash);
        String databaseName = splitUrl[splitUrl.length - 1];
        databaseName = databaseName.concat(version);

        databaseName = createNewDatabase(statement, databaseName);
        URL = URL.concat("/" + databaseName);
        Connection connectionToNewDatabase = DriverManager.getConnection(URL, connection.getMetaData().getUserName(), password);
        logger.info("Creating Tables in new Database...");
        executeSqlScript(connectionToNewDatabase, tableScriptFile, false);
        logger.info("Generating Data from JSON file");
        String insertQuery = generateInsertQuery(version);
        logger.info("Inserting Data in new Database...");
        executeSqlScript(connectionToNewDatabase, insertQuery, true);
        logger.info("Altering Tables and adding Constraints...");
        executeSqlScript(connectionToNewDatabase, constraintsFile, false);

        logger.info("Database sucessfully recovered from backup");
        connection.close();
    }

    private String createNewDatabase(Statement statement, String databaseName) throws SQLException {
        do {
            try {
                statement.execute("""
                        CREATE DATABASE %s OWNER %s
                        """.formatted(databaseName, connection.getMetaData().getUserName()));
                logger.info("Succesfully created new Database with name %s".formatted(databaseName));
                break;
            } catch (PSQLException e) {
                logger.warn(e.getMessage());
                if (databaseName.length() >= 63) throw new RuntimeException("The Databasename got too long please delete some copys and retry!");
                databaseName = databaseName.concat("copy");
            }
        }while (true);
        return databaseName;
    }

    private List<Map<String, Object>> generateInsertQueryFromJson(String filePath) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            List<Map<String, Object>> data = objectMapper.readValue(Files.readAllBytes(Paths.get(filePath)), new TypeReference<>() {
            });
            return data;
        }catch (IOException e) {
            logger.error(e.getMessage() + " " + e.getStackTrace());
            throw new RuntimeException(e.getMessage());
        }
    }

    private String generateInsertQuery(String version){
        String[] tableNames = getTableNames();
        if (tableNames == null) {
            return null;
        }
        StringBuilder queryBuilder = new StringBuilder();

        for (String tableName : tableNames) {
            List<Map<String, Object>> data = null;
            if (MetaDataController.getInstance().getCurrentVersionName().equals(version)) {
                String filePathToCurrentData = getFilePathToCurrentData(tableName, version);
                data = generateInsertQueryFromJson(filePathToCurrentData);
            }else {
                IncrementalHelper incrementalHelper = new IncrementalHelper();
                try {
                    data = incrementalHelper.getBackupCurrentData(version, tableName);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
            if (data.isEmpty()) continue; // Keine Daten vorhanden, weiter zur nächsten Tabelle

            StringBuilder columns = new StringBuilder();
            boolean columnsSet = false;

            for (Map<String, Object> row : data) {
                if (!row.containsKey("object")) continue; // Falls kein "object"-Key existiert, überspringen
                Map<String, Object> actualData = (Map<String, Object>) row.get("object"); // Echte Tabellenwerte extrahieren

                StringBuilder values = new StringBuilder("(");

                for (Map.Entry<String, Object> entry : actualData.entrySet()) {

                    if (!columnsSet) {
                        if (columns.length() > 0) columns.append(", ");
                        columns.append(entry.getKey());
                    }

                    values.append(formatValue(entry.getValue())).append(", ");
                }

                values.setLength(values.length() - 2); // Entferne letztes Komma und Leerzeichen
                values.append(")");

                if (!columnsSet) {
                    columns.insert(0, "(").append(")");
                    columnsSet = true;
                }

                queryBuilder.append("INSERT INTO ").append('"').append(tableName).append('"')
                        .append(" ").append(columns)
                        .append(" VALUES ").append(values)
                        .append(";")
                        .append(System.lineSeparator());
            }
        }
        logger.info("Succesfully generated Insert comands");
        return queryBuilder.toString();
    }

    private String getFilePathToCurrentData(String tableName, String version) {
        if (MetaDataController.getInstance().getCurrentVersionName().equals(version)){
            return MetaDataController.getInstance().getTableCurrentPath(tableName);
        }
        return null;
    }


    private String[] getTableNames() {
        String filePath = MetaDataController.getInstance().getObjectsFilePath();
        File folder = new File(filePath);
        File[] tableFolders = folder.listFiles(File::isDirectory);

        if (tableFolders == null) {
            logger.error("No data for import found!");
            return null;
        }
        String[] tableNames = new String[tableFolders.length];
        for (int i = 0; i < tableFolders.length; i++) {
            String tableName = tableFolders[i].getName();
            tableNames[i] = tableName;
        }
        return tableNames;
    }



    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'"; // String mit Escaping
        } else if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE"; // Boolean direkt in SQL-Format
        } else if (value instanceof Number) {
            if (isPossibleTimestamp((Number) value)) {
                return "'" + convertTimestampToDate((Number) value) + "'"; // Datum richtig formatieren
            }
            return value.toString(); // Zahlen direkt übernehmen (ohne Anführungszeichen)
        } else {
            return "'" + value.toString().replace("'", "''") + "'"; // Fallback als String
        }
    }

    // Prüft, ob die Zahl ein UNIX-Timestamp ist (Millisekunden seit 1970)
    private boolean isPossibleTimestamp(Number value) {
        long timestamp = value.longValue();
        return timestamp > 1_000_000_000L; // Prüft, ob größer als Sekunden-Timestamp
    }

    // Konvertiert Timestamp in SQL-Datum (YYYY-MM-DD)
    private String convertTimestampToDate(Number value) {
        Date date = new Date(value.longValue());
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }


    private void executeSqlScript(Connection connection, String filePathOrQuery, boolean isQuery) {
        try {
            String sql;
            if (!isQuery) {
                sql = new String(Files.readAllBytes(Paths.get(filePathOrQuery)));
            }else {
                sql = filePathOrQuery;
            }
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
            logger.info("SQL-script succesfully executed!");
        } catch (IOException | SQLException e) {
            logger.error("Error at execution of SQL-scripts:" + " - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
