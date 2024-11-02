package org.example.JDBC;


import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;


public class JSONExporter {
    private String url;
    private String user;
    private String password;

    public JSONExporter(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public void exportDataToJSON() {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            Statement stmt = conn.createStatement();
            ResultSet tables = stmt.executeQuery(tableQuery);

            while (tables.next()) {
                String tableName = tables.getString("table_name");

                // Neues Statement für den Datenabruf erstellen
                try (Statement dataStmt = conn.createStatement()) {
                    List<Map<String, Object>> data = fetchDataFromTable(dataStmt, tableName);
                    writeDataToJSON(tableName, data);
                }
            }
            tables.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<Map<String, Object>> fetchDataFromTable(Statement stmt, String tableName) throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        // Tabelle in doppelte Anführungszeichen setzen, um Reservierte Wörter zu behandeln
        String dataQuery = "SELECT * FROM \"" + tableName + "\"";
        ResultSet resultSet = stmt.executeQuery(dataQuery);
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(metaData.getColumnName(i), resultSet.getObject(i));
            }
            data.add(row);
        }
        resultSet.close();
        return data;
    }

    private void writeDataToJSON(String tableName, List<Map<String, Object>> data) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            File jsonFile = new File("./src/main/java/org/example/TestData/" + tableName + ".json");
            objectMapper.writeValue(jsonFile, data);
            System.out.println("Daten für Tabelle " + tableName + " erfolgreich in " + jsonFile.getName() + " exportiert.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}