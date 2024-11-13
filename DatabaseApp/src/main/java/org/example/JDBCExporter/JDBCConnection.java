package org.example.JDBCExporter;

import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnection {
    static Logger logger = new Logger();

    private static final String url = "jdbc:postgresql://localhost:5432/my_database";
    private static final String user = "user";
    private static final String password = "password";

    public static void main(String[] args) throws SQLException {
        try(Connection connection = DriverManager.getConnection(url, user, password)) {
            logger.info("JDBC Connection successfully");

            // Object Exporter
            ObjectExporter objectExporter = new ObjectExporter(connection);
            objectExporter.exportData();

            // Schema Exporter
            SchemaExporter schemaExporter = new SchemaExporter(connection);

            String tableScript = schemaExporter.generateTableScript();
            String constraintsScript = schemaExporter.generateConstraintsScript();
            logger.info("Schema generated successfully");

            FileWriter fileWriter = new FileWriter();
            fileWriter.createDirectory("./src/main/java/org/example/TestData/tableSchema");
            fileWriter.createDirectory("./src/main/java/org/example/TestData/Constraints");
            fileWriter.writeFile("./src/main/java/org/example/TestData/tableSchema/create_tables.sql", tableScript);
            fileWriter.writeFile("./src/main/java/org/example/TestData/Constraints/add_constraints.sql", constraintsScript);
            logger.info("SQL Files created successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}