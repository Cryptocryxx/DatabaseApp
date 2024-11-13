package org.example.JDBCExporter;

import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class JDBCConnection {
    static Logger logger = new Logger();
    static FileWriter fileWriter = new FileWriter();


    private static final String url = "jdbc:postgresql://localhost:5432/my_database";
    private static final String user = "user";
    private static final String password = "password";

    public static void main(String[] args) throws SQLException {
        try(Connection connection = DriverManager.getConnection(url, user, password)) {
            MetaDataExporter metaDataExporter = new MetaDataExporter();
            String version = metaDataExporter.getNextVersion();

            logger.info("JDBC Connection successfully");

            // Object Exporter
            String filePath = "./src/main/java/org/example/TestData/Objects";
            fileWriter.createDirectory(filePath);
            ObjectExporter objectExporter = new ObjectExporter(connection, filePath);
            Map<String, String> dataFiles = objectExporter.exportData(version);

            // Schema Exporter
            SchemaExporter schemaExporter = new SchemaExporter(connection);

            String tableScript = schemaExporter.generateTableScript();
            String constraintsScript = schemaExporter.generateConstraintsScript();
            logger.info("Schema generated successfully");

            String tableScriptPath = "./src/main/java/org/example/TestData/tableSchema/create_tables_"+version+".sql";
            String constraintsPath = "./src/main/java/org/example/TestData/Constraints/add_constraints_"+version+".sql";

            fileWriter.createDirectory("./src/main/java/org/example/TestData/tableSchema");
            fileWriter.createDirectory("./src/main/java/org/example/TestData/Constraints");
            fileWriter.writeFile(tableScriptPath, tableScript);
            fileWriter.writeFile(constraintsPath, constraintsScript);
            metaDataExporter.exportMetaData(version, tableScriptPath, constraintsPath, dataFiles);
            logger.info("SQL Files created successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}