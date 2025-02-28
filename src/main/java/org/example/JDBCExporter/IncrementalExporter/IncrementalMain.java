package org.example.JDBCExporter.IncrementalExporter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.JDBCExporter.FileWriter;
import org.example.JDBCExporter.MetaDataController;
import org.example.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class IncrementalMain {

    static Logger logger = new Logger();
    static FileWriter fileWriter = new FileWriter();
    IncrementalHelper incrementalHelper = new IncrementalHelper();
    /**
     * Verarbeitet alle Tabellen und aktualisiert die Current-Datei basierend auf der Basisdatei und den inkrementellen Dateien.
     */
    public void processTables() throws IOException {
        MetaDataController metaDataController = MetaDataController.getInstance();

        try {
            // Hole die Namen aller Tabellen aus den Metadaten
            List<String> tableNames = metaDataController.getTableNamesFromMetadata();

            for (String tableName : tableNames) {
                logger.info("Processing table: "+ tableName);

                // 1️⃣ Lade die Basisdatei
                String basePath = metaDataController.getTableBasePath(tableName);
                logger.info("Loading base file from: "+ basePath);
                List<Map<String, Object>> currentData = incrementalHelper.loadDataFromFile(basePath);
                logger.info("Base data loaded: "+ currentData);

                // 2️⃣ Lade alle inkrementellen Dateien in der richtigen Reihenfolge
                List<String> incrementalPaths = metaDataController.getIncrementalFiles(tableName);
                logger.info("Incremental paths: "+ incrementalPaths);

                // 3️⃣ Speichere den aktuellen Zustand in der Current-Datei
                String currentPath = metaDataController.getTableCurrentPath(tableName);
                logger.info("Saving current data to: "+ currentPath);
                fileWriter.writeJSONFile(currentPath, currentData);
                logger.info("Current data saved: "+ currentData);
                incrementalHelper.applyIncrementalChanges(incrementalPaths, currentData);
                // 4️⃣ Vergleiche die Current-Datei mit der neuesten inkrementellen Datei (falls vorhanden)
                incrementalHelper.compareCurrentToIncremental(incrementalPaths, currentData, currentPath);
            }
        } catch (Exception e) {
            logger.error("Error processing tables: " + e.getMessage());
        }
    }

}
