package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.*;
import org.example.JDBCExporter.IncrementalExporter.IncrementalMain;
import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class CreateBackupHelper {
    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;
    MetaDataController metaDataController = MetaDataController.getInstance();

    public CreateBackupHelper(Connection connection, Logger logger, FileWriter fileWriter) {
        this.connection = connection;
        this.logger = logger;
        this.fileWriter = fileWriter;
    }

    public void performBackup() throws SQLException, IOException {
        metaDataController.updateMetaData(connection);

        exportObjects();
        exportSchema();

        if(metaDataController.getCurrentVersion() > 1){
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
        incrementalMain.processTables(metaDataController.getMetaDataFilePath(), metaDataController.getLastVersionName(), metaDataController.getCurrentVersionName());
    }
}
