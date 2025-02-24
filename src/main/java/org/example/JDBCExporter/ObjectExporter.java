package org.example.JDBCExporter;

import org.example.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class ObjectExporter {
    Logger logger = new Logger();
    FileWriter fileWriter = new FileWriter();

    private final Connection connection;
    private final String filePath;

    public ObjectExporter(Connection connection, String filePath){
        this.connection = connection;
        this.filePath = filePath;
    }

    public Map<String, String> exportData(String version) throws SQLException {
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
                fileWriter.createDirectory(filePath + "/" + tableName);
                String newFilePath = String.format("%s/%s_%s.json", filePath + "/" + tableName, tableName, version);
                fileWriter.writeJSONFile(newFilePath, data);
                dataFiles.put(tableName, newFilePath);
            }
            logger.info("Data export to JSON completed successfully.");
        } catch (IOException e) {
            logger.error(String.format("SQL Error during data export: %s", e.getMessage()));
        }
        return dataFiles;
    }

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
                    row.put(metaData.getColumnName(i), tables.getObject(i));
                }
                data.add(row);
            }
            logger.info(String.format("Data fetched successfully from table: %s", tableName));
        }
        return data;
    }

    public void importData(String version) {
        logger.info("Beginne den Import von JSON-Daten in die Datenbank...");
        File folder = new File(filePath);
        File[] tableFolders = folder.listFiles(File::isDirectory);

        if (tableFolders == null) {
            logger.error("Keine Daten zum Importieren gefunden.");
            return;
        }

        for (File tableFolder : tableFolders) {
            String tableName = tableFolder.getName();
            File jsonFile = new File(tableFolder, tableName + "_" + version + ".json");

            if (!jsonFile.exists()) {
                logger.warn("Keine Datei für Tabelle " + tableName + " gefunden.");
                continue;
            }

            try {
                List<Map<String, Object>> data = fileWriter.readJSONFile(jsonFile.getPath());
                insertDataIntoTable(tableName, data);
                logger.info("Daten für Tabelle " + tableName + " erfolgreich importiert.");
            } catch (SQLException e) {
                logger.error("Fehler beim Importieren von Daten für Tabelle " + tableName + ": " + e.getMessage());
            }
        }
    }

    private void insertDataIntoTable(String tableName, List<Map<String, Object>> data) throws SQLException {
        if (data.isEmpty()) return;

        StringBuilder queryBuilder = new StringBuilder("INSERT INTO \"").append(tableName).append("\" (");
        Map<String, Object> firstRow = data.get(0);

        List<String> columns = new ArrayList<>(firstRow.keySet());
        queryBuilder.append(String.join(", ", columns)).append(") VALUES (");
        queryBuilder.append(String.join(", ", Collections.nCopies(columns.size(), "?"))).append(")");

        try (PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString())) {
            for (Map<String, Object> row : data) {
                for (int i = 0; i < columns.size(); i++) {
                    stmt.setObject(i + 1, row.get(columns.get(i)));
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
    }
}
