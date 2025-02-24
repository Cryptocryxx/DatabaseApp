package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.FileWriter;
import org.example.JDBCExporter.IncrementalExporter.IncrementalMain;
import org.example.JDBCExporter.MetaData;
import org.example.JDBCExporter.ObjectExporter;
import org.example.JDBCExporter.SchemaExporter;
import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class CreateBackupHelper {
    // file path constants
    private final String META_FILE_PATH = "./src/main/java/org/example/TestData/metadata.json";
    private final String TABLE_FILE_PATH = "./src/main/java/org/example/TestData/tableSchema";
    private final String CONSTRAINTS_FILE_PATH = "./src/main/java/org/example/TestData/Constraints";
    private final String OBJECTS_FILE_PATH = "./src/main/java/org/example/TestData/Objects";

    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;
    private final MetaData metaData = MetaData.getInstance();

    public CreateBackupHelper(Connection connection, Logger logger, FileWriter fileWriter) {
        this.connection = connection;
        this.logger = logger;
        this.fileWriter = fileWriter;
    }

    public void performBackup() throws SQLException, IOException {
        String version = metaData.getNextVersion();

        Map<String, String> dataFiles = exportObjects(version);
        exportSchema(version);

        metaData.exportMetaData(version, getTableScriptPath(version), getConstraintsPath(version), dataFiles);

        if(Integer.parseInt(version.substring(1)) > 1){
            exportIncremental(version);
        }
    }

    private Map<String, String> exportObjects(String version) throws SQLException {
        fileWriter.createDirectory(OBJECTS_FILE_PATH);
        ObjectExporter objectExporter = new ObjectExporter(connection, OBJECTS_FILE_PATH);

        return objectExporter.exportData(version);
    }

    private void exportSchema(String version) throws SQLException, IOException {
        SchemaExporter schemaExporter = new SchemaExporter(connection);
        String tableScript = schemaExporter.generateTableScript();
        String constraintsScript = schemaExporter.generateConstraintsScript();

        logger.info("Schema erfolgreich generiert.");

        fileWriter.createDirectory(TABLE_FILE_PATH);
        fileWriter.createDirectory(CONSTRAINTS_FILE_PATH);
        fileWriter.writeFile(getTableScriptPath(version), tableScript);
        fileWriter.writeFile(getConstraintsPath(version), constraintsScript);

        logger.info("Schema Dateien erfolgreich erstellt.");
    }

    // perform incremental Backup if a previous version exist
    private void exportIncremental(String version) throws SQLException, IOException {
        int newVersionNum = Integer.parseInt(version.substring(1));
        int oldVersionNum = newVersionNum - 1;
        String oldVersion = "v" + oldVersionNum;
        String newVersion = "v" + newVersionNum;

        logger.debug("Alte und neue Version: " + oldVersion + " " + newVersion);

        IncrementalMain incrementalMain = new IncrementalMain();
        incrementalMain.processTables(META_FILE_PATH, oldVersion, newVersion);
    }

    private String getTableScriptPath(String version) {
        return TABLE_FILE_PATH + "/create_tables_" + version + ".sql";
    }

    private String getConstraintsPath(String version) {
        return CONSTRAINTS_FILE_PATH + "/add_constraints_" + version + ".sql";
    }
}
