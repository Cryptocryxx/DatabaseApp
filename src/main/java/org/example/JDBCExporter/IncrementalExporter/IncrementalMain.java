package org.example.JDBCExporter.IncrementalExporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.JDBCExporter.FileWriter;
import org.example.JDBCExporter.ObjectExporter;
import org.example.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;

import static org.example.JDBCExporter.JDBCConnection.*;

public class IncrementalMain {

    static Logger logger = new Logger();
    static FileWriter fileWriter = new FileWriter();
    static ObjectMapper objectMapper = new ObjectMapper();

    public Map<String, Object> compareData(List<Map<String, Object>> oldData, List<Map<String, Object>> newData) {
        logger.info("OldData: " + oldData);
        logger.info("NewData: " + newData);

        List<Map<String, Object>> added = new ArrayList<>();
        List<String> deleted = new ArrayList<>();

        // 1️⃣ Erstelle eine Map von alten Daten, wobei der Index als Schlüssel dient
        Map<String, Map<String, Object>> oldDataMap = new HashMap<>();
        for (Map<String, Object> oldRow : oldData) {
            oldDataMap.put((String) oldRow.get("index"), oldRow);
        }

        // 2️⃣ Neue Einträge hinzufügen, falls sie nicht existieren
        for (Map<String, Object> newRow : newData) {
            boolean exists = false;

            // Vergleiche nur das "object" und nicht das gesamte Map
            for (Map<String, Object> oldRow : oldData) {
                if (Objects.equals(oldRow.get("object"), newRow.get("object"))) {
                    exists = true;
                    break;
                }
            }

            // Wenn das Objekt nicht gefunden wurde, wird es als neues Element betrachtet
            if (!exists) {
                Map<String, Object> newObject = new HashMap<>();
                newObject.put("index", UUID.randomUUID().toString()); // Generiere einen neuen Index
                newObject.put("object", newRow.get("object")); // Füge das Objekt hinzu
                added.add(newObject);
            }
        }

        // 3️⃣ Gelöschte Einträge finden (falls sie nicht mehr in newData existieren)
        for (Map<String, Object> oldRow : oldData) {
            boolean exists = false;

            // Wenn das Objekt aus oldData in newData existiert, überspringe es
            for (Map<String, Object> newRow : newData) {
                if (Objects.equals(oldRow.get("object"), newRow.get("object"))) {
                    exists = true;
                    break;
                }
            }

            // Falls das alte Objekt nicht mehr existiert, speichern wir nur den Index
            if (!exists) {
                deleted.add((String) oldRow.get("index"));
            }
        }

        // 4️⃣ Rückgabe der Änderungen (nur added und deleted, ohne "index" oder "object")
        Map<String, Object> changes = new HashMap<>();
        changes.put("added", added);
        changes.put("deleted", deleted);

        logger.info("Changes without index/object: " + changes);  // Ausgabe zum Debuggen

        return changes;
    }

    public void processTables(String metaFilePath, String oldVersion, String newVersion) throws IOException {

        //Loading metadata
        Map<String, Object> metaData = objectMapper.readValue(new File(metaFilePath), Map.class);
        Map<String, String> dataFiles = (Map<String, String>) metaData.get("data_files");

        try {
            //Connection connection = DriverManager.getConnection(url, user, password);
            //ObjectExporter objectExporter = new ObjectExporter(connection, "./src/main/java/org/example/TestData/Objects");

            for (Map.Entry<String, String> entry : dataFiles.entrySet()) {
                String tableName = entry.getKey();
                String newFilePath = entry.getValue();
                String oldFilePath = newFilePath.replace(newVersion, oldVersion);

                List<Map<String, Object>> oldData = Arrays.asList(objectMapper.readValue(new File(oldFilePath), Map[].class));
                List<Map<String, Object>> newData = Arrays.asList(objectMapper.readValue(new File(newFilePath), Map[].class));
                //fileWriter.writeJSONFile(newFilePath, newData);

                String incrementalFilePath = newFilePath.replace(newVersion, "incremental_" + newVersion);

                Map<String, Object> changes = compareData(oldData, newData);

                // Verpacken Sie die Map in eine Liste, um sie an writeJSONFile zu übergeben
                fileWriter.writeJSONFile(incrementalFilePath, changes);
                logger.info(String.format("Incremental changes for table '%s' saved to %s", tableName, incrementalFilePath));

                if (new File(newFilePath).delete()) {
                    logger.info(String.format("File '%s' deleted successfully after processing.", newFilePath));
                } else {
                    logger.warn(String.format("File '%s' could not be deleted.", newFilePath));
                }
            }

        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

}
