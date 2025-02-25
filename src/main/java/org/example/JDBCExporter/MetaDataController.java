package org.example.JDBCExporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.example.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaDataController {
    private static final Logger logger = new Logger();
    private static final FileWriter fileWriter = new FileWriter();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // path constants
    private static final String BASE_PATH = "./src/main/java/org/example/TestData";
    private static final String META_FILE_PATH = BASE_PATH + "/metadata.json";
    private static final String CONSTRAINTS_PATH = "/Constraints";
    private static final String TABLE_SCHEMA_PATH = "/tableSchema";
    private static final String OBJECTS_PATH = "/Objects";
    private static final org.apache.logging.log4j.Logger log = LogManager.getLogger(MetaDataController.class);

    private Map<String, Object> metadata;

    private static MetaDataController instance;

    public MetaDataController() {
        File metaFile = new File(META_FILE_PATH);

        if (metaFile.exists()) {
            try {
                metadata = objectMapper.readValue(metaFile, Map.class);
            } catch (IOException e) {
                logger.error("Failed to load existing metadata: " + e.getMessage());
                metadata = new HashMap<>();
            }
        }else{
            metadata = new HashMap<>();
        }

        checkMetaFile();
    }

    public static synchronized MetaDataController getInstance() {
        if (instance == null) {
            instance = new MetaDataController();
        }
        return instance;
    }

    private void checkMetaFile(){
        metadata.putIfAbsent("path", BASE_PATH);
        if (!metadata.containsKey("constraints")) {
            Map<String, Object> constraints = new HashMap<>();
            constraints.put("path", CONSTRAINTS_PATH);
            constraints.put("versions", new ArrayList<String>());
            metadata.put("constraints", constraints);
        }
        if (!metadata.containsKey("tableSchema")) {
            Map<String, Object> tableSchema = new HashMap<>();
            tableSchema.put("path", TABLE_SCHEMA_PATH);
            tableSchema.put("versions", new ArrayList<String>());
            metadata.put("tableSchema", tableSchema);
        }
        if (!metadata.containsKey("objects")) {
            Map<String, Object> objects = new HashMap<>();
            objects.put("path", OBJECTS_PATH);
            metadata.put("objects", objects);
        }

        fileWriter.createDirectory(BASE_PATH);
        fileWriter.createDirectory(BASE_PATH + CONSTRAINTS_PATH);
        fileWriter.createDirectory(BASE_PATH + TABLE_SCHEMA_PATH);
        fileWriter.createDirectory(BASE_PATH + OBJECTS_PATH);
    }

    public void updateMetaData(Connection connection){
        // Update constraints versions
        Map<String, Object> constraints = (Map<String, Object>) metadata.get("constraints");
        List<String> constraintsVersions = (List<String>) constraints.get("versions");

        int nextVersionNumber = constraintsVersions.isEmpty()
                ? 1
                : getCurrentVersion(constraintsVersions.get(constraintsVersions.size() - 1)) + 1;
        String nextVersionStr = "v" + nextVersionNumber;

        String nextConstraintVersionFile = "add_constraints_" + nextVersionStr + ".sql";
        constraintsVersions.add(nextConstraintVersionFile);
        constraints.put("versions", constraintsVersions);
        metadata.put("constraints", constraints);

        // Update tableSchema versions
        Map<String, Object> tableSchema = (Map<String, Object>) metadata.get("tableSchema");
        List<String> tableSchemaVersions = (List<String>) tableSchema.get("versions");
        String nextTableSchemaVersionFile = "create_tables_" + nextVersionStr + ".sql";
        tableSchemaVersions.add(nextTableSchemaVersionFile);
        tableSchema.put("versions", tableSchemaVersions);
        metadata.put("tableSchema", tableSchema);

        // Update objects
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");
        Map<String, Object> updatedObjects = new HashMap<>();
        updatedObjects.put("path", objects.get("path"));

        List<String> tableNames = getTableNamesFromDB(connection);
        for(String tableName : tableNames) {
            if (objects.containsKey(tableName)) {
                Map<String, Object> tableEntry = (Map<String, Object>) objects.get(tableName);
                List<String> incrementalList = (List<String>) tableEntry.get("incremental");
                if (incrementalList == null) {
                    incrementalList = new ArrayList<>();
                }

                String nextIncremental = tableName + "_incremental_" + nextVersionStr + ".json";
                incrementalList.add(nextIncremental);
                tableEntry.put("current", tableName + "_current.json");
                updatedObjects.put(tableName, tableEntry);
            }else{
                Map<String, Object> tableEntry = new HashMap<>();
                tableEntry.put("path", "/" + tableName);
                tableEntry.put("basis", tableName + "_v1.json");

                List<String> incrementalList = new ArrayList<>();
                tableEntry.put("incremental", incrementalList);
                tableEntry.put("current", tableName + "_v1.json");
                updatedObjects.put(tableName, tableEntry);

                fileWriter.createDirectory(BASE_PATH + OBJECTS_PATH + "/" + tableName);
            }
        }
        metadata.put("objects", updatedObjects);
    }

    public void save() throws IOException {
        File metaFile = new File(META_FILE_PATH);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(metaFile, metadata);
        logger.info("Saved metadata file at: " + metaFile.getAbsolutePath());
    }

    private List<String> getTableNamesFromDB(Connection connection) {
        List<String> tableNames = new ArrayList<>();
        String query = String.format("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'""");

        try (Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)) {
            while (tables.next()) {
                tableNames.add(tables.getString("table_name"));
            }
        } catch (Exception e) {
            logger.error("Error fetching table names: " + e.getMessage());
        }

        return tableNames;
    }

    public int getCurrentVersion(String filename) {
        int version = 1;
        try {
            int vIndex = filename.indexOf("v");
            int dotIndex = filename.indexOf(".", vIndex);
            String numberStr = filename.substring(vIndex + 1, dotIndex);
            version = Integer.parseInt(numberStr);
        } catch (Exception e) {
            logger.warn("Could not get version from " + filename + ". Defaulting to 1.");
        }
        return version;
    }

    public Integer getCurrentVersion() {
        Map<String, Object> constraints = (Map<String, Object>) metadata.get("constraints");
        String constraintsPath = (String) constraints.get("path");
        List<String> versions = (List<String>) constraints.get("versions");

        return versions.size() - 1;
    }

    public String getCurrentVersionName() {
        Map<String, Object> constraints = (Map<String, Object>) metadata.get("constraints");
        String constraintsPath = (String) constraints.get("path");
        List<String> versions = (List<String>) constraints.get("versions");

        return "v" + String.valueOf(versions.size() - 1);
    }

    public String getLastVersionName() {
        Map<String, Object> constraints = (Map<String, Object>) metadata.get("constraints");
        List<String> versions = (List<String>) constraints.get("versions");
        if (versions.isEmpty()) {
            return "";
        }
        String lastVersionFile = versions.get(versions.size() - 1);
        int vIndex = lastVersionFile.indexOf("v");
        int dotIndex = lastVersionFile.indexOf(".", vIndex);
        if (vIndex != -1 && dotIndex != -1) {
            return lastVersionFile.substring(vIndex, dotIndex);
        } else {
            return "";
        }
    }

    public String getMetaDataFilePath() {
        return META_FILE_PATH;
    }

    public String getObjectsFilePath() {
        return BASE_PATH + OBJECTS_PATH;
    }

    public String getConstraintsFilePath() {
        return getConstraintsFilePath(null);
    }

    public String getTableSchemaFilePath() {
        return getTableSchemaFilePath(null);
    }

    public String getConstraintsFilePath(String version) {
        Map<String, Object> constraints = (Map<String, Object>) metadata.get("constraints");
        List<String> versions = (List<String>) constraints.get("versions");

        String fileName;
        if (version == null || version.isEmpty()) {
            fileName = versions.get(versions.size() - 1);
        } else {
            fileName = versions.stream()
                    .filter(v -> v.contains(version))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No constraints file found for version " + version));
        }

        return BASE_PATH + CONSTRAINTS_PATH + "/" + fileName;
    }

    public String getTableSchemaFilePath(String version) {
        Map<String, Object> tableSchema = (Map<String, Object>) metadata.get("tableSchema");
        List<String> versions = (List<String>) tableSchema.get("versions");

        String fileName;
        if (version == null || version.isEmpty()) {
            fileName = versions.get(versions.size() - 1);
        } else {
            fileName = versions.stream()
                    .filter(v -> v.contains(version))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("No tableSchema file found for version " + version));
        }

        return BASE_PATH + TABLE_SCHEMA_PATH + "/" + fileName;
    }

    public String getObjectFilePath(String tableName) {
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");

        for (String objectName : objects.keySet()) {
            logger.info(objectName);
        }

        if (!objects.containsKey(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " not found in metadata");
        }

        Map<String, Object> tableEntry = (Map<String, Object>) objects.get(tableName);
        String fileName;
        List<String> incrementalList = (List<String>) tableEntry.get("incremental");

        if (incrementalList != null && !incrementalList.isEmpty()) {
            fileName = incrementalList.get(incrementalList.size() - 1);
        } else {
            fileName = (String) tableEntry.get("basis");
        }

        return BASE_PATH + OBJECTS_PATH + "/" + tableName + "/" + fileName;
    }
}
