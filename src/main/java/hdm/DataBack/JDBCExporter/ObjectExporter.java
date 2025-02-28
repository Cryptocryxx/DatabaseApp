package hdm.DataBack.JDBCExporter;

import hdm.DataBack.Logger.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class ObjectExporter {
    Logger logger = new Logger();
    FileWriter fileWriter = new FileWriter();
    MetaDataController metaDataController = MetaDataController.getInstance();

    private final Connection connection;

    /**
     * Constructor to initialize ObjectExporter with a database connection
     *
     * @param connection the database connection
     */
    public ObjectExporter(Connection connection, String filePath){
        this.connection = connection;
    }

    /**
     * Exports data from all tables in the public schema to JSON files.
     *
     * @return a map where the key is the table name and the value is the file path of the exported JSON
     * @throws SQLException if a database access error occurs
     */
    public Map<String, String> exportData() throws SQLException {
        Map<String, String> dataFiles = new HashMap<>();
        logger.info("Starting exporting Data to JSON...");
        String query = String.format("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'""");

        try (Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)) {
            while (tables.next()) {
                String tableName = tables.getString("table_name");
                List<Map<String, Object>> data = fetchDataFromTable(tableName);
                String newFilePath = metaDataController.getTableFilePath(tableName);

                fileWriter.writeJSONFile(newFilePath, data);
                dataFiles.put(tableName, newFilePath);
            }
            logger.info("Data export to JSON completed successfully.");
        } catch (IOException e) {
            logger.error(String.format("SQL Error during data export: %s", e.getMessage()));
        }
        return dataFiles;
    }

    /**
     * Fetches all data from a specified table.
     *
     * @param tableName the name of the table to fetch data from
     * @return a list of maps representing the table's data, where each map corresponds to a row
     * @throws SQLException if a database access error occurs
     */
    public List<Map<String, Object>> fetchDataFromTable(String tableName) throws SQLException {
        logger.info(String.format("Fetching data from table: %s", tableName));
        String query = String.format("SELECT * FROM \"%s\"", tableName);

        List<Map<String, Object>> data = new ArrayList<>();

        try (Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)) {
            ResultSetMetaData metaData = tables.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (tables.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = tables.getObject(i);


                    if (value instanceof java.sql.Date) {
                        java.sql.Date sqlDate = (java.sql.Date) value;
                        row.put(columnName, sqlDate.toString());
                    } else if (value instanceof java.sql.Timestamp) {
                        java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
                        row.put(columnName, timestamp.toLocalDateTime().toString());
                    } else {
                        row.put(columnName, value); // Keep other values as-is
                    }
                }
                data.add(row);
            }
            logger.info(String.format("Data fetched successfully from table: %s", tableName));
        }
        return data;
    }
}
