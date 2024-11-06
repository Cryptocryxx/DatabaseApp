package org.example.JDBCExporter;

import org.example.JDBC.JSONExporter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnection {
    private static final String url = "jdbc:postgresql://localhost:5432/my_database";
    private static final String user = "user";
    private static final String password = "password";

    public static void main(String[] args) throws SQLException {
        // Json Exporter
        JSONExporter jsonExporter = new JSONExporter(url, user, password);
        jsonExporter.exportDataToJSON();

        // Schema Exporter
        try(Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("JDBC Connection successfully");

            SchemaExporter schemaExporter = new SchemaExporter(connection);

            String tableScript = schemaExporter.generateTableScript();
            String constraintsScript = schemaExporter.generateConstraintsScript();

            System.out.println("Schema generated successfully");

            FileExporter fileExporter = new FileExporter();
            fileExporter.exportToFile("./src/main/java/org/example/TestData/create_tables.sql", tableScript);
            fileExporter.exportToFile("./src/main/java/org/example/TestData/add_constraints.sql", constraintsScript);

            System.out.println("SQL Files created successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
