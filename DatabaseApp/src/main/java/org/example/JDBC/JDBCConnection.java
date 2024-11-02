package org.example.JDBC;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCConnection {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/my_database";
        String user = "user";
        String password = "password";

        StringBuilder tableScript = new StringBuilder();
        StringBuilder constraintsScript = new StringBuilder();

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            String tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            Statement stmt = conn.createStatement();
            ResultSet tables = stmt.executeQuery(tableQuery);

            List<String> tableNames = new ArrayList<>();
            while (tables.next()) {
                tableNames.add(tables.getString("table_name"));
            }
            tables.close();

            for (String tableName : tableNames) {
                tableScript.append("CREATE TABLE ").append(tableName).append(" (\n");
                String columnQuery = "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = '" + tableName + "'";
                ResultSet columns = stmt.executeQuery(columnQuery);
                List<String> columnDefinitions = new ArrayList<>();

                while (columns.next()) {
                    String columnName = columns.getString("column_name");
                    String dataType = columns.getString("data_type");
                    String defaultVal = (columns.getString("column_default") != null && !columns.getString("column_default").contains("nextval")) ? " DEFAULT " + columns.getString("column_default") : "";
                    columnDefinitions.add(columnName + " " + mapDataType(dataType) + defaultVal);

                    // NOT NULL-Constraints in constraintsScript verschieben
                    if (columns.getString("is_nullable").equals("NO")) {
                        constraintsScript.append("ALTER TABLE ").append(tableName).append(" ALTER COLUMN ")
                                .append(columnName).append(" SET NOT NULL;\n");
                    }
                }
                columns.close();

                tableScript.append(String.join(",\n", columnDefinitions)).append("\n);\n\n");

                // Hier Constraints für Fremdschlüssel, UNIQUE, CHECK usw. sammeln
                collectConstraints(stmt, tableName, constraintsScript);
            }

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Skripte in Dateien schreiben
        try (FileWriter tableFile = new FileWriter("create_tables.sql");
             FileWriter constraintFile = new FileWriter("add_constraints.sql")) {
            tableFile.write(tableScript.toString());
            constraintFile.write(constraintsScript.toString());
            System.out.println("SQL-Skripte erfolgreich erstellt.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String mapDataType(String dataType) {
        switch (dataType) {
            case "integer": return "INT";
            case "character varying": return "VARCHAR(255)";
            case "timestamp without time zone": return "TIMESTAMP";
            // Weitere Zuordnungen für allgemeine Datentypen hinzufügen
            default: return dataType;
        }
    }

    private static void collectConstraints(Statement stmt, String tableName, StringBuilder constraintsScript) throws SQLException {
        // Primärschlüssel
        String pkQuery = "SELECT kcu.column_name FROM information_schema.key_column_usage kcu " +
                "JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name " +
                "WHERE kcu.table_name = '" + tableName + "' AND tc.constraint_type = 'PRIMARY KEY'";
        ResultSet pkResult = stmt.executeQuery(pkQuery);
        if (pkResult.next()) {
            constraintsScript.append("ALTER TABLE ").append(tableName).append(" ADD PRIMARY KEY (")
                    .append(pkResult.getString("column_name")).append(");\n");
        }
        pkResult.close();

        // Fremdschlüssel
        String fkQuery = "SELECT kcu.constraint_name, kcu.column_name, ccu.table_name AS referenced_table, " +
                "ccu.column_name AS referenced_column " +
                "FROM information_schema.key_column_usage kcu " +
                "JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name " +
                "JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name " +
                "WHERE kcu.table_name = '" + tableName + "' AND tc.constraint_type = 'FOREIGN KEY'";
        ResultSet fkResult = stmt.executeQuery(fkQuery);
        while (fkResult.next()) {
            constraintsScript.append("ALTER TABLE ").append(tableName).append(" ADD CONSTRAINT ")
                    .append(fkResult.getString("constraint_name")).append(" FOREIGN KEY (")
                    .append(fkResult.getString("column_name")).append(") REFERENCES ")
                    .append(fkResult.getString("referenced_table")).append("(")
                    .append(fkResult.getString("referenced_column")).append(");\n");
        }
        fkResult.close();

        // UNIQUE-Constraints
        String uniqueQuery = "SELECT tc.constraint_name, kcu.column_name " +
                "FROM information_schema.table_constraints tc " +
                "JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name " +
                "WHERE tc.table_name = '" + tableName + "' AND tc.constraint_type = 'UNIQUE'";
        ResultSet uniqueResult = stmt.executeQuery(uniqueQuery);
        while (uniqueResult.next()) {
            constraintsScript.append("ALTER TABLE ").append(tableName).append(" ADD CONSTRAINT ")
                    .append(uniqueResult.getString("constraint_name")).append(" UNIQUE (")
                    .append(uniqueResult.getString("column_name")).append(");\n");
        }
        uniqueResult.close();

        // CHECK-Constraints - Anpassung für richtige Abfrage des Tabellennamens
        String checkQuery = "SELECT cc.constraint_name, cc.check_clause " +
                "FROM information_schema.check_constraints cc " +
                "JOIN information_schema.constraint_table_usage ctu ON cc.constraint_name = ctu.constraint_name " +
                "WHERE ctu.table_name = '" + tableName + "'";
        ResultSet checkResult = stmt.executeQuery(checkQuery);
        while (checkResult.next()) {
            constraintsScript.append("ALTER TABLE ").append(tableName).append(" ADD CONSTRAINT ")
                    .append(checkResult.getString("constraint_name")).append(" CHECK (")
                    .append(checkResult.getString("check_clause")).append(");\n");
        }
        checkResult.close();
    }
}
