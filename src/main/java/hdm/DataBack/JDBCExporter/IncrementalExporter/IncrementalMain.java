package hdm.DataBack.JDBCExporter.IncrementalExporter;

import hdm.DataBack.JDBCExporter.FileWriter;
import hdm.DataBack.JDBCExporter.MetaDataController;
import hdm.DataBack.Logger.Logger;

import java.io.IOException;
import java.util.*;

public class IncrementalMain {

    static Logger logger = new Logger();
    static FileWriter fileWriter = new FileWriter();
    IncrementalHelper incrementalHelper = new IncrementalHelper();

    /**
     * Processes all tables by updating the current data file based on the base file and incremental backups.
     * - Loads the base file for each table.
     * - Applies all incremental changes in the correct order.
     * - Saves the latest state to the current data file.
     * - Compares the current data file with the latest incremental backup to detect further changes.
     *
     * @throws IOException if an error occurs while reading or writing files
     */
    public void processTables() throws IOException {
        MetaDataController metaDataController = MetaDataController.getInstance();

        try {
            // Hole die Namen aller Tabellen aus den Metadaten
            List<String> tableNames = metaDataController.getTableNames();

            for (String tableName : tableNames) {
                logger.info("Processing table: "+ tableName);

                // Lade die Basisdatei
                String basePath = metaDataController.getTableBaseFilePath(tableName);
                logger.info("Loading base file from: "+ basePath);
                List<Map<String, Object>> currentData = incrementalHelper.loadDataFromFile(basePath);
                logger.info("Base data loaded: "+ currentData);

                // Lade alle inkrementellen Dateien in der richtigen Reihenfolge
                List<String> incrementalPaths = metaDataController.getIncrementalFilePath(tableName);
                logger.info("Incremental paths: "+ incrementalPaths);

                // Speichere den aktuellen Zustand in der Current-Datei
                String currentPath = metaDataController.getTableCurrentFilePath(tableName);
                logger.info("Saving current data to: "+ currentPath);
                fileWriter.writeJSONFile(currentPath, currentData);
                logger.info("Current data saved: "+ currentData);
                incrementalHelper.applyIncrementalChanges(incrementalPaths, currentData);
                // Vergleiche die Current-Datei mit der neuesten inkrementellen Datei (falls vorhanden)
                incrementalHelper.compareCurrentToIncremental(incrementalPaths, currentData, currentPath);
            }
        } catch (Exception e) {
            logger.error("Error processing tables: " + e.getMessage());
        }
    }

}
