package org.example.JDBCExporter;

import org.example.Logger.Logger;

import javax.naming.spi.DirectoryManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCConnection {
    Logger logger = new Logger();
    private static final String url = "jdbc:postgresql://localhost:5432/my_database";
    private static final String user = "user";
    private static final String password = "password";

    public static void main(String[] args) throws SQLException {
        // Json Exporter
        JSONExporter jsonExporter = new JSONExporter(url, user, password);
        jsonExporter.exportDataToJSON();
        JDBCConnection manager = new JDBCConnection();

        // Schema Exporter
        try(Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("JDBC Connection successfully");

            SchemaExporter schemaExporter = new SchemaExporter(connection);

            String tableScript = schemaExporter.generateTableScript();
            String constraintsScript = schemaExporter.generateConstraintsScript();

            System.out.println("Schema generated successfully");
            manager.createSqlDir();
            FileExporter fileExporter = new FileExporter();
            fileExporter.exportToFile("./src/main/java/org/example/TestData/tableSchema/create_tables.sql", tableScript);
            fileExporter.exportToFile("./src/main/java/org/example/TestData/Constraints/add_constraints.sql", constraintsScript);

            System.out.println("SQL Files created successfully");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void createSqlDir(){
        String pathTableSchema = "./src/main/java/org/example/TestData/tableSchema";
        String pathTableConstraints = "./src/main/java/org/example/TestData/Constraints";

        Path path1 = Paths.get(pathTableSchema);
        Path path2 = Paths.get(pathTableConstraints);

        try{
            Files.createDirectories(path1);
            Files.createDirectories(path2);
            logger.info("Directory created successfully");
        } catch (IOException e) {
            logger.error("Fehler beim Erstellen der Ordner: "+ e.getMessage());
        }

    }
}
