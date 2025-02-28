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


public class IncrementalHelper {
    static Logger logger = new Logger();
    static FileWriter fileWriter = new FileWriter();
    static ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Compares the current data with the latest incremental backup and updates the incremental files accordingly.
     *
     * @param incrementalPaths list of file paths to incremental backup files
     * @param currentData the latest current data from the database
     * @param currentPath the file path to store updated current data
     * @throws IOException if an error occurs while reading or writing files
     */
    public void compareCurrentToIncremental(List<String> incrementalPaths, List<Map<String, Object>> currentData, String currentPath) throws IOException {
        if (!incrementalPaths.isEmpty()) {
            String latestIncrementalPath = incrementalPaths.get(incrementalPaths.size() - 1);
            File latestIncrementalFile = new File(latestIncrementalPath);
            logger.info("Latest incremental file:"+ latestIncrementalPath);

            if (latestIncrementalFile.exists()) {
                if (isIncrementalChangesFile(latestIncrementalFile)) {
                    logger.info("Latest file contains incremental changes.");
                    // Wenn die neueste inkrementelle Datei Änderungen enthält
                    Map<String, Object> incrementalChanges = loadIncrementalChangesFromFile(latestIncrementalPath);
                    applyIncrementalChangesInFile(currentData, incrementalChanges);
                } else {
                    logger.info("Latest file contains full data.");
                    // Wenn die neueste inkrementelle Datei vollständige Daten enthält
                    List<Map<String, Object>> latestIncrementalData = loadDataFromFile(latestIncrementalPath);
                    logger.info("Latest incremental data loaded: "+ latestIncrementalData);
                    logger.info("Current data: "+currentData);
                    Map<String, Object> changes = compareData(currentData, latestIncrementalData);
                    applyIncrementalChangesInFile(currentData, changes);
                    // Überschreibe die neueste inkrementelle Datei mit den Änderungen
                    fileWriter.writeJSONFile(latestIncrementalPath, changes);
                    logger.info("Incremental changes saved to: "+ latestIncrementalPath);

                    // Aktualisiere die Current-Datei mit dem neuesten Zustand
                    logger.info("Latest Incremental data saved: "+ latestIncrementalData);
                    fileWriter.writeJSONFileWithoutIndex(currentPath, currentData);
                    logger.info("Current data updated with latest incremental data.");
                }
            } else {
                logger.warn("Latest incremental file does not exist: "+ latestIncrementalPath);
            }
        }
    }

    /**
     * Applies incremental changes from a list of incremental backup files to the current dataset.
     *
     * @param incrementalPaths list of file paths to incremental backup files
     * @param currentData the dataset to update with incremental changes
     * @throws IOException if an error occurs while reading files
     */
    void applyIncrementalChanges(List<String> incrementalPaths, List<Map<String, Object>> currentData) throws IOException {
        for (String path : incrementalPaths) {
            File file = new File(path);
            logger.info("Processing incremental file: "+ path);

            if (file.exists()) {
                // Überprüfe, ob die Datei Änderungen oder vollständige Daten enthält
                if (isIncrementalChangesFile(file)) {
                    logger.info("File contains incremental changes.");
                    Map<String, Object> incrementalChanges = loadIncrementalChangesFromFile(path);
                    applyIncrementalChangesInFile(currentData, incrementalChanges);
                } else {
                    logger.info("File contains full data.");

                }
                logger.info("Current data after processing,"+ path +", Current data: "+ currentData);
            } else {
                logger.warn("File does not exist: , "+ path);
            }
        }
    }

    /**
     * Retrieves the reconstructed data for a specific backup version of a table.
     *
     * @param targetVersion the target backup version to retrieve
     * @param tableName the name of the table to restore
     * @return a list of maps representing the table's reconstructed data
     * @throws IOException if an error occurs while reading files
     */
    public List<Map<String, Object>> getBackupCurrentData(String targetVersion, String tableName) throws IOException {
        MetaDataController metaDataController = MetaDataController.getInstance();
        Map<String, List<Map<String, Object>>> allTablesData = new HashMap<>();

        // Lade die Basisdatei
        String basePath = metaDataController.getTableBaseFilePath(tableName);
        logger.info("Loading base file from: " + basePath);
        List<Map<String, Object>> BackupCurrentData = loadDataFromFile(basePath);
        logger.info("Base data loaded for table " + tableName + ": " + BackupCurrentData);

        // Lade alle inkrementellen Dateien
        List<String> incrementalPaths = metaDataController.getIncrementalFilePath(tableName);
        logger.info("Incremental paths for table " + tableName + ": " + incrementalPaths);

        for (String path : incrementalPaths) {
            File file = new File(path);
            logger.info("Processing incremental file: " + path);

            if (file.exists()) {
                // Extrahiere die Versionsnummer aus dem Dateinamen
                String fileName = file.getName();
                String version = fileName.substring(fileName.lastIndexOf("v") + 1);

                // Stoppe, wenn die Zielversion erreicht ist
                if (version.equals(targetVersion)) {
                    logger.info("Reached target version: " + targetVersion + " for table " + tableName + ", stopping.");
                    break;
                }

                if (isIncrementalChangesFile(file)) {
                    logger.info("File contains incremental changes.");
                    Map<String, Object> incrementalChanges = loadIncrementalChangesFromFile(path);
                    applyIncrementalChangesInFile(BackupCurrentData, incrementalChanges);
                } else {
                    logger.info("File contains full data.");
                    List<Map<String, Object>> fullData = loadDataFromFile(path);
                    BackupCurrentData = new ArrayList<>(fullData);
                }

                logger.info("Temporary current data after processing " + path + " for table " + tableName + ": " + BackupCurrentData);
            } else {
                logger.warn("File does not exist: " + path);
            }
        }

        return BackupCurrentData;
    }

    /**
     * Determines whether a file contains incremental changes rather than full data.
     *
     * @param file the file to check
     * @return true if the file contains incremental changes, false otherwise
     * @throws IOException if an error occurs while reading the file
     */
    private boolean isIncrementalChangesFile(File file) throws IOException {
        if (!file.exists()) {
            return false; // Datei existiert nicht
        }

        try {
            // Lese die gesamte Datei als JSON-Knoten ein
            JsonNode rootNode = objectMapper.readTree(file);

            // Überprüfe, ob es sich um ein Objekt (Map) mit den Schlüsseln "added" und "deleted" handelt
            if (rootNode.isObject()) {
                return rootNode.has("added") && rootNode.has("deleted");
            }

            // Wenn es sich um ein Array handelt, ist es keine inkrementelle Änderungsdatei
            return false;
        } catch (IOException e) {
            logger.error("Error reading file: " + file.getPath() + ", " + e.getMessage());
            return false; // Bei einem Fehler wird angenommen, dass es sich nicht um eine inkrementelle Änderungsdatei handelt
        }
    }

    /**
     * Loads table data from a JSON file.
     *
     * @param filePath the path to the JSON file
     * @return a list of maps representing the table data
     * @throws IOException if an error occurs while reading the file
     */
    List<Map<String, Object>> loadDataFromFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            // Verwende TypeReference, um den Typ der Map explizit anzugeben
            return objectMapper.readValue(file, new TypeReference<List<Map<String, Object>>>() {});
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Loads incremental changes from a JSON file.
     *
     * @param filePath the path to the JSON file
     * @return a map containing added and deleted data changes
     * @throws IOException if an error occurs while reading the file
     */
    private Map<String, Object> loadIncrementalChangesFromFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            return objectMapper.readValue(file, Map.class);
        } else {
            return new HashMap<>();
        }
    }

    /**
     * Applies incremental changes to the current dataset.
     *
     * @param currentData the dataset to update
     * @param changes a map containing added and deleted data changes
     */
    private void applyIncrementalChangesInFile(List<Map<String, Object>> currentData, Map<String, Object> changes) {
        // Lösche gelöschte Einträge
        List<String> deletedIndices = (List<String>) changes.getOrDefault("deleted", new ArrayList<>());
        currentData.removeIf(entry -> deletedIndices.contains(entry.get("index")));

        // Füge neue Einträge hinzu
        List<Map<String, Object>> addedEntries = (List<Map<String, Object>>) changes.getOrDefault("added", new ArrayList<>());
        for (Map<String, Object> addedEntry : addedEntries) {
            // Überprüfe, ob der Eintrag bereits in currentData existiert
            boolean exists = currentData.stream()
                    .anyMatch(entry -> Objects.equals(entry.get("object"), addedEntry.get("object")));

            if (!exists) {
                // Wenn der Eintrag neu ist, füge ihn mit einem neuen Index hinzu
                Map<String, Object> newEntry = new HashMap<>();
                newEntry.put("index", addedEntry.get("index")); // Neuer Index
                newEntry.put("object", addedEntry.get("object"));
                currentData.add(newEntry);
            }
        }
    }

    /**
     * Compares old and new datasets and identifies differences.
     *
     * @param oldData the previous dataset
     * @param newData the new dataset
     * @return a map containing lists of added and deleted records
     */
    public Map<String, Object> compareData(List<Map<String, Object>> oldData, List<Map<String, Object>> newData) {
        List<Map<String, Object>> added = new ArrayList<>();
        List<String> deleted = new ArrayList<>();

        // Erstelle eine Map von alten Daten, wobei der Index als Schlüssel dient
        Map<String, Map<String, Object>> oldDataMap = new HashMap<>();
        for (Map<String, Object> oldRow : oldData) {
            oldDataMap.put((String) oldRow.get("index"), oldRow);
        }

        // Neue Einträge hinzufügen, falls sie nicht existieren
        for (Map<String, Object> newRow : newData) {
            boolean exists = oldData.stream()
                    .anyMatch(oldRow -> Objects.equals(oldRow.get("object"), newRow.get("object")));

            if (!exists) {
                added.add(newRow);
            }
        }

        // Gelöschte Einträge finden
        for (Map<String, Object> oldRow : oldData) {
            boolean exists = newData.stream()
                    .anyMatch(newRow -> Objects.equals(oldRow.get("object"), newRow.get("object")));

            if (!exists) {
                deleted.add((String) oldRow.get("index"));
            }
        }

        // Rückgabe der Änderungen
        Map<String, Object> changes = new HashMap<>();
        changes.put("added", added);
        changes.put("deleted", deleted);
        return changes;
    }
}
