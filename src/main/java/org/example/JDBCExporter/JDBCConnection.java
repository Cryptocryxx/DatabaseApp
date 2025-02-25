package org.example.JDBCExporter;

import org.example.JDBCExporter.IncrementalExporter.IncrementalMain;
import org.example.Logger.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class JDBCConnection {
    static Logger logger = new Logger();
    static FileWriter fileWriter = new FileWriter();

    public void doBackup(String url, String user, String password){
        try(Connection connection = DriverManager.getConnection(url, user, password)) {
            MetaData metaData = MetaData.getInstance();
            String version = metaData.getNextVersion();

            logger.info("JDBC Connection successfully");

            // Object Exporter
            String filePath = "./src/main/java/org/example/TestData/Objects";
            fileWriter.createDirectory(filePath);
            ObjectExporter objectExporter = new ObjectExporter(connection, filePath);
            Map<String, String> dataFiles = objectExporter.exportData();

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
            metaData.exportMetaData(version, tableScriptPath, constraintsPath, dataFiles);
            logger.info("SQL Files created successfully");
            int newVersionNum = Integer.parseInt(version.substring(1));
            int oldVersionNum = newVersionNum-1;
            logger.info("Old Version Number: "+ oldVersionNum);
            logger.info("New Version Number: " + newVersionNum);
            if(newVersionNum > 1){
                IncrementalMain incrementalMain = new IncrementalMain();
                String oldVersion = "v"+oldVersionNum;
                String newVersion = "v"+newVersionNum;
                logger.debug("Old and New Version: " + oldVersion + " "+ newVersion);
                String metaFilePath = "./src/main/java/org/example/TestData/metadata.json";
                incrementalMain.processTables();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException("You already made an initial backup. If you want to make a new one, please delete the old version.");
        }
    }
    public void restoreBackup(String url, String user, String password) {
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            logger.info("Verbindung zur Datenbank hergestellt.");

            // Backup-Verzeichnis definieren
            String tableScriptPath = "./src/main/java/org/example/TestData/tableSchema";
            String constraintsPath = "./src/main/java/org/example/TestData/Constraints";
            String dataFilesPath = "./src/main/java/org/example/TestData/Objects";

            // Aktuelle Version ermitteln
            MetaData metaData = MetaData.getInstance();
            String version = metaData.getCurrentVersion();

            // SQL-Dateien auslesen
            String tableScriptFile = tableScriptPath + "/create_tables_" + version + ".sql";
            String constraintsFile = constraintsPath + "/add_constraints_" + version + ".sql";

            Statement statement = connection.createStatement();
            String[] splitUrl = url.split("/");
            String databaseName = splitUrl[splitUrl.length - 1];
            statement.execute("DROP DATABASE " + databaseName);
            statement.execute("");
            // SQL-Skripte ausführen
            executeSqlScript(connection, tableScriptFile);
            executeSqlScript(connection, constraintsFile);

            // Daten wiederherstellen
            ObjectExporter objectExporter = new ObjectExporter(connection, dataFilesPath);
            objectExporter.importData(version);

            logger.info("Backup erfolgreich wiederhergestellt.");
        } catch (SQLException e) {
            logger.error("Fehler beim Wiederherstellen des Backups: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private void executeSqlScript(Connection connection, String filePath) {
        try {
            String sql = new String(Files.readAllBytes(Paths.get(filePath)));
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
            }
            logger.info("SQL-Skript ausgeführt: " + filePath);
        } catch (IOException | SQLException e) {
            logger.error("Fehler beim Ausführen des SQL-Skripts: " + filePath + " - " + e.getMessage());
            throw new RuntimeException(e);
        }
    }



    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/my_database";
        String user = "user";
        String password = "password";
        JDBCConnection connection = new JDBCConnection();
        connection.doBackup(url, user, password);
    }
}