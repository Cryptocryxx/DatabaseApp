package org.example.Hibernate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

public class HibernateConnection {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/my_database"; // Setze hier deine DB-URL
        String user = "user"; // Setze hier deinen Benutzernamen
        String password = "password"; // Setze hier dein Passwort

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // Schritt 1: Tabellennamen abrufen
            String tableQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            Statement stmt = conn.createStatement();
            ResultSet tables = stmt.executeQuery(tableQuery);
            List<String> tableNames = new ArrayList<>();
            while (tables.next()) {
                tableNames.add(tables.getString("table_name"));
            }
            tables.close();

            // Schritt 2: Für jede Tabelle Spalten und Metadaten abrufen
            for (String tableName : tableNames) {
                System.out.println("Tabelle: " + tableName);

                // Spaltennamen abrufen
                String columnQuery = "SELECT column_name FROM information_schema.columns WHERE table_name = '" + tableName + "'";
                ResultSet columns = stmt.executeQuery(columnQuery);
                List<String> columnNames = new ArrayList<>();
                while (columns.next()) {
                    columnNames.add(columns.getString("column_name"));
                }
                columns.close();

                // Metadaten für jede Spalte abrufen
                for (String columnName : columnNames) {
                    Map<String, Object> constraintsInfo = new HashMap<>();

                    // Primary Key
                    String pkQuery = "SELECT kcu.constraint_name FROM information_schema.key_column_usage kcu " +
                            "JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name " +
                            "WHERE kcu.table_name = '" + tableName + "' AND kcu.column_name = '" + columnName + "' " +
                            "AND tc.constraint_type = 'PRIMARY KEY'";
                    ResultSet pkResult = stmt.executeQuery(pkQuery);
                    while (pkResult.next()) {
                        constraintsInfo.put("Primary Key", pkResult.getString("constraint_name"));
                    }
                    pkResult.close();

                    // Foreign Key
                    String fkQuery = "SELECT kcu.constraint_name, kcu.table_name AS source_table, " +
                            "kcu.column_name AS source_column, rc.unique_constraint_name, " +
                            "kcu2.table_name AS referenced_table, kcu2.column_name AS referenced_column " +
                            "FROM information_schema.key_column_usage kcu " +
                            "JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name " +
                            "JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name " +
                            "JOIN information_schema.key_column_usage kcu2 ON rc.unique_constraint_name = kcu2.constraint_name " +
                            "WHERE kcu.table_name = '" + tableName + "' AND kcu.column_name = '" + columnName + "' " +
                            "AND tc.constraint_type = 'FOREIGN KEY'";
                    ResultSet fkResult = stmt.executeQuery(fkQuery);
                    List<Map<String, String>> fks = new ArrayList<>();
                    while (fkResult.next()) {
                        Map<String, String> fkInfo = new HashMap<>();
                        fkInfo.put("constraint_name", fkResult.getString("constraint_name"));
                        fkInfo.put("source_table", fkResult.getString("source_table"));
                        fkInfo.put("source_column", fkResult.getString("source_column"));
                        fkInfo.put("referenced_table", fkResult.getString("referenced_table"));
                        fkInfo.put("referenced_column", fkResult.getString("referenced_column"));
                        fks.add(fkInfo);
                    }
                    fkResult.close();
                    constraintsInfo.put("Foreign Keys", fks);

                    // Unique Constraints
                    String uniqueQuery = "SELECT tc.constraint_name FROM information_schema.table_constraints tc " +
                            "WHERE tc.table_name = '" + tableName + "' AND tc.constraint_type = 'UNIQUE'";
                    ResultSet uniqueResult = stmt.executeQuery(uniqueQuery);
                    List<String> uniqueConstraints = new ArrayList<>();
                    while (uniqueResult.next()) {
                        uniqueConstraints.add(uniqueResult.getString("constraint_name"));
                    }
                    uniqueResult.close();
                    constraintsInfo.put("Unique Constraints", uniqueConstraints);

                    System.out.println("Spalte: " + columnName + " - Constraints und Keys: " + constraintsInfo);
                }
            }
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
