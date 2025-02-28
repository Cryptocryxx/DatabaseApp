package hdm.DataBack.JDBCExporter.Backup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import hdm.DataBack.JDBCExporter.IncrementalExporter.IncrementalHelper;
import hdm.DataBack.JDBCExporter.MetaDataController;
import hdm.DataBack.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class RestoreBackup {

    private final Connection connection;
    private final Logger logger;
    private String version;

    /**
     * Constructor for RestoreBackup that initializes a database connection and logger.
     *
     * @param connection the database connection
     * @param logger the logger instance for logging operations
     */
    public RestoreBackup(Connection connection, Logger logger) {
        this.connection = connection;
        this.logger = logger;
    }

    /**
     * Restores the database from a specified backup version.
     * - Deletes existing tables and data.
     * - Recreates tables using the stored schema.
     * - Inserts data from the backup files.
     * - Restores constraints.
     *
     * @param version the version identifier in the format "v1"
     * @throws SQLException if a database access error occurs
     */
    public void performRestore(String version) throws SQLException {
        logger.info("Verbindung zur Datenbank hergestellt.");
        this.version = version;

        String tableScriptFile = MetaDataController.getInstance().getTableSchemaFilePath(version);
        String constraintsFile = MetaDataController.getInstance().getConstraintsFilePath(version);

        DeleteDataFromDatabase();
        logger.info("Creating Tables in Database...");
        executeSqlScript(connection, tableScriptFile, false);
        logger.info("Generating Data from JSON file");
        String insertQuery = generateInsertQuery(version);
        logger.info("Inserting Data in Database...");
        executeSqlScript(connection, insertQuery, true);
        logger.info("Altering Tables and adding Constraints...");
        executeSqlScript(connection, constraintsFile, false);

        logger.info("Database sucessfully recovered from backup");
        connection.close();
    }

    /**
     * Deletes all tables in the database with CASCADE, removing all existing data.
     *
     * @throws SQLException if a database access error occurs
     */
    private void DeleteDataFromDatabase() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        List<String> tableNames = new ArrayList<>();

        try (ResultSet rs = metaData.getTables(null, null, "%", new String[] {"TABLE"})) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                tableNames.add(tableName);
            }
        }

        try (Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                statement.execute(String.format("""
                        DROP TABLE IF EXISTS \"%s\" CASCADE
                        """, tableName));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads JSON data from a file and converts it into a list of maps representing table rows.
     *
     * @param filePath the path to the JSON file
     * @return a list of maps containing table data
     */
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

    /**
     * Generates an SQL insert query from JSON backup data.
     *
     * @param version the backup version to restore
     * @return a string containing the SQL insert queries for all tables
     */
    private String generateInsertQuery(String version) {
        List<String> tableNames = MetaDataController.getInstance().getTableNames();
        if (tableNames == null) {
            return null;
        }
        StringBuilder queryBuilder = new StringBuilder();

        for (String tableName : tableNames) {
            List<Map<String, Object>> data = null;
            if (MetaDataController.getInstance().getCurrentVersionName().equals(version)) {
                String filePathToCurrentData = getFilePathToCurrentData(tableName, version);
                data = generateInsertQueryFromJson(filePathToCurrentData);
            } else {
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
                    String columnName = entry.getKey();

                    // Überprüfen, ob die Spalte SERIAL ist
                    try {
                        if (isSerialColumn(tableName, columnName)) {
                            continue; // Spalte überspringen, wenn sie SERIAL ist
                        }
                    } catch (SQLException e) {
                        logger.error("Fehler beim Überprüfen des Spaltentyps: " + e.getMessage());
                        throw new RuntimeException(e);
                    }

                    if (!columnsSet) {
                        if (columns.length() > 0) columns.append(", ");
                        columns.append(columnName);
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
        logger.info("Successfully generated INSERT commands");
        return queryBuilder.toString();
    }

    /**
     * Retrieves the file path to the current data of a given table.
     *
     * @param tableName the name of the table
     * @param version the backup version to restore
     * @return the file path to the current JSON backup file
     */
    private String getFilePathToCurrentData(String tableName, String version) {
        if (MetaDataController.getInstance().getCurrentVersionName().equals(version)){
            return MetaDataController.getInstance().getTableCurrentFilePath(tableName);
        }
        return null;
    }

    /**
     * Formats a value for safe SQL insertion.
     *
     * @param value the value to format
     * @return the formatted SQL-compatible string
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'"; // String mit Escaping
        } else if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE"; // Boolean direkt in SQL-Format
        } else if (value instanceof Number) {
            return value.toString(); // Zahlen direkt übernehmen (ohne Anführungszeichen)
        } else {
            return "'" + value.toString().replace("'", "''") + "'"; // Fallback als String
        }
    }

    /**
     * Checks if a column is of type SERIAL.
     *
     * @param tableName the name of the table
     * @param columnName the name of the column
     * @return true if the column is of type SERIAL, false otherwise
     * @throws SQLException if a database access error occurs
     */
    private boolean isSerialColumn(String tableName, String columnName) throws SQLException {
        String query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = ?
            AND column_name = ?
            AND data_type = 'integer'
            AND column_default LIKE 'nextval%'
        );
        """;

        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
            }
        }
        return false;
    }

    /**
     * Executes an SQL script or query on the database.
     *
     * @param connection the database connection
     * @param filePathOrQuery the file path to an SQL script or the SQL query itself
     * @param isQuery whether the input is a direct SQL query (true) or a file path (false)
     */
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
