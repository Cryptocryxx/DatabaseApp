package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.MetaData;
import org.example.JDBCExporter.ObjectExporter;
import org.example.Logger.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class RestoreBackupHelper {

    private final Connection connection;
    private final Logger logger;

    public RestoreBackupHelper(Connection connection, Logger logger ) {
        this.connection = connection;
        this.logger = logger;
    }

    public void performRestore() throws SQLException {
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
        String[] splitUrl = connection.getMetaData().getURL().split("/");
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
}
