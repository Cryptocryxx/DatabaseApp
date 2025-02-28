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

    /**
     * Constructor to initialize CreateBackup with a database connection, logger, and file writer.
     *
     * @param connection the database connection
     * @param logger the logger instance for logging operations
     * @param fileWriter the file writer for saving backup files
     */
    public CreateBackup(Connection connection, Logger logger, FileWriter fileWriter) {
        this.connection = connection;
        this.logger = logger;
        this.fileWriter = fileWriter;
    }

    /**
     * Performs a full database backup by exporting data, schema, and incremental changes.
     * - Exports table data to JSON files.
     * - Saves table schema and constraints as SQL scripts.
     * - If a previous backup exists, stores only the incremental changes.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while writing backup files
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


    /**
     * Exports data from all tables in the database into JSON files.
     *
     * @throws SQLException if a database access error occurs
     */
    private void exportObjects() throws SQLException {
        ObjectExporter objectExporter = new ObjectExporter(connection, metaDataController.getObjectsFilePath());
        objectExporter.exportData();
    }

    /**
     * Exports the database schema, including table structures and constraints, into SQL script files.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while writing schema files
     */
    private void exportSchema() throws SQLException, IOException {
        SchemaExporter schemaExporter = new SchemaExporter(connection);
        String tableScript = schemaExporter.generateTableScript();
        String constraintsScript = schemaExporter.generateConstraintsScript();

        logger.info("Schema erfolgreich generiert.");

        fileWriter.writeFile(metaDataController.getTableSchemaFilePath(), tableScript);
        fileWriter.writeFile(metaDataController.getConstraintsFilePath(), constraintsScript);

        logger.info("Schema Dateien erfolgreich erstellt.");
    }

    /**
     * Performs an incremental backup by identifying and storing only the changes from the previous backup.
     * This method is executed only if a previous backup version exists.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while processing incremental backups
     */
    private void exportIncremental() throws SQLException, IOException {
        IncrementalMain incrementalMain = new IncrementalMain();
        incrementalMain.processTables();
    }
}
