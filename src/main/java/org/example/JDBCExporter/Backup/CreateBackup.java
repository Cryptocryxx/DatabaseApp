package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.*;
import org.example.JDBCExporter.IncrementalExporter.IncrementalMain;
import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateBackup {
    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;
    MetaDataController metaDataController = MetaDataController.getInstance();

    public CreateBackup(Connection connection, Logger logger, FileWriter fileWriter) {
        this.connection = connection;
        this.logger = logger;
        this.fileWriter = fileWriter;
    }

    /**
     * Performs backup by first getting the data of the database and storing it in custom folders.
     * After that the tables and constraints of the tables gets stored in form of sql commands in a .sql file
     * Lastly if this is not the first backup the stored data is compared to earlier version and only the changes get stored.
     * @throws SQLException
     * @throws IOException
     */
    public void performBackup() throws SQLException, IOException {
        metaDataController.updateMetaData(connection);

        exportObjects();
        exportSchema();
        logger.info("Current version: " + metaDataController.getCurrentVersion());
        if(metaDataController.getCurrentVersion() > 0){
            exportIncremental();
        }

        metaDataController.save();
    }

    private void exportObjects() throws SQLException {
        ObjectExporter objectExporter = new ObjectExporter(connection, metaDataController.getObjectsFilePath());
        objectExporter.exportData();
    }

    private void exportSchema() throws SQLException, IOException {
        SchemaExporter schemaExporter = new SchemaExporter(connection);
        String tableScript = schemaExporter.generateTableScript();
        String constraintsScript = schemaExporter.generateConstraintsScript();

        logger.info("Schema erfolgreich generiert.");

        fileWriter.writeFile(metaDataController.getTableSchemaFilePath(), tableScript);
        fileWriter.writeFile(metaDataController.getConstraintsFilePath(), constraintsScript);

        logger.info("Schema Dateien erfolgreich erstellt.");
    }

    // perform incremental Backup if a previous version exist
    private void exportIncremental() throws SQLException, IOException {
        IncrementalMain incrementalMain = new IncrementalMain();
        incrementalMain.processTables();
    }
}
