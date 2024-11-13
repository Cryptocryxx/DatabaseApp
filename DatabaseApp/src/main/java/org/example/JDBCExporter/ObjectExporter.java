package org.example.JDBCExporter;

import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObjectExporter {
    Logger logger = new Logger();
    FileWriter fileWriter = new FileWriter();

    private final Connection connection;
    private final String filePath;

    public ObjectExporter(Connection connection, String filePath){
        this.connection = connection;
        this.filePath = filePath;
    }

    public void exportData() throws SQLException {
        logger.info("Starting exporting Data to JSON...");
        String query = String.format("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'""");

        try (Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)) {
            while (tables.next()) {
                String tableName = tables.getString("table_name");
                List<Map<String, Object>> data = fetchDataFromTable(tableName);

                String newFilePath = String.format("%s/%s.json", filePath, tableName);
                fileWriter.writeJSONFile(newFilePath, data);
            }
            logger.info("Data export to JSON completed successfully.");
        } catch (IOException e) {
            logger.error(String.format("SQL Error during data export: %s", e.getMessage()));
        }
    }

    private List<Map<String, Object>> fetchDataFromTable(String tableName) throws SQLException {
        logger.info(String.format("Fetching data from table: %s", tableName));
        String query = String.format("SELECT * FROM \"%s\"", tableName);

        List<Map<String, Object>> data = new ArrayList<>();

        try (Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)) {
            ResultSetMetaData metaData = tables.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (tables.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), tables.getObject(i));
                }
                data.add(row);
            }
            logger.info(String.format("Data fetched successfully from table: %s", tableName));
        }
        return data;
    }
}
