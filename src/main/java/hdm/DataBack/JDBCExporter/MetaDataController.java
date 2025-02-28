package hdm.DataBack.JDBCExporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import hdm.DataBack.Logger.Logger;

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
    private final Logger logger = new Logger();
    private final FileWriter fileWriter = new FileWriter();
    private final ObjectMapper objectMapper = new ObjectMapper();

    // path constants
    private final String BASE_PATH = "./BackupData";
    private final String META_FILE_PATH = BASE_PATH + "/metadata.json";
    private final String CONSTRAINTS_PATH = "/Constraints";
    private final String TABLE_SCHEMA_PATH = "/tableSchema";
    private final String OBJECTS_PATH = "/Objects";
    private final String OBJECTS_TABLES_PATH = "/tables";

    private Map<String, Object> metadata;

    private static MetaDataController instance;

    private MetaDataController() {
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

    /**
     * Returns the singleton instance of MetaDataController.
     * Ensures thread-safe lazy initialization.
     *
     * @return instance of MetaDataController
     */
    public static synchronized MetaDataController getInstance() {
        if (instance == null) {
            instance = new MetaDataController();
        }
        return instance;
    }

    /**
     * Ensures that the metadata file is initialized properly with required structures.
     * Creates necessary directories if they do not exist.
     */
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
            objects.put("tables", new HashMap<String, Object>());
            metadata.put("objects", objects);
        }

        fileWriter.createDirectory(BASE_PATH);
        fileWriter.createDirectory(BASE_PATH + CONSTRAINTS_PATH);
        fileWriter.createDirectory(BASE_PATH + TABLE_SCHEMA_PATH);
        fileWriter.createDirectory(BASE_PATH + OBJECTS_PATH);
        fileWriter.createDirectory(BASE_PATH + OBJECTS_PATH + OBJECTS_TABLES_PATH);
    }

    /**
     * Updates metadata with the latest constraints, table schema versions, and table objects.
     *
     * @param connection the database connection to fetch table names
     */
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
        Map<String, Object> tableObjects = (Map<String, Object>) objects.get("tables");

        List<String> tableNames = getTableNamesFromDB(connection);
        for(String tableName : tableNames) {
            if (tableObjects.containsKey(tableName)) {
                Map<String, Object> tableEntry = (Map<String, Object>) tableObjects.get(tableName);
                List<String> incrementalList = (List<String>) tableEntry.get("incremental");
                if (incrementalList == null) {
                    incrementalList = new ArrayList<>();
                }

                String nextIncremental = tableName + "_incremental_" + nextVersionStr + ".json";
                incrementalList.add(nextIncremental);
                tableEntry.put("current", tableName + "_current.json");
                tableObjects.put(tableName, tableEntry);
            }else{
                Map<String, Object> tableEntry = new HashMap<>();
                tableEntry.put("path", "/" + tableName);
                tableEntry.put("base", tableName + "_" + nextVersionStr + ".json");

                List<String> incrementalList = new ArrayList<>();
                tableEntry.put("incremental", incrementalList);
                tableEntry.put("current", tableName + "_" + nextVersionStr + ".json");
                tableObjects.put(tableName, tableEntry);

                fileWriter.createDirectory(BASE_PATH + OBJECTS_PATH + OBJECTS_TABLES_PATH + "/" + tableName);
            }
        }
        objects.put("tables", tableObjects);
        metadata.put("objects", objects);
    }

    /**
     * Saves the metadata to the metadata.json file.
     *
     * @throws IOException if writing to the file fails
     */
    public void save() throws IOException {
        File metaFile = new File(META_FILE_PATH);
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(metaFile, metadata);
        logger.info("Saved metadata file at: " + metaFile.getAbsolutePath());
    }

    /**
     * Retrieves table names from the database schema.
     *
     * @param connection the database connection
     * @return list of table names in the public schema
     */
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

    /**
     * Extracts the version number from a given filename.
     *
     * @param filename the filename containing a version
     * @return extracted version number, defaults to 1 if extraction fails
     */
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

    /**
     * Retrieves the current version index from metadata.
     *
     * @return the latest version index
     */
    public Integer getCurrentVersion() {
        Map<String, Object> constraints = (Map<String, Object>) metadata.get("constraints");
        List<String> versions = (List<String>) constraints.get("versions");

        return versions.size() - 1;
    }


    /**
     * Retrieves the current version name from metadata.
     *
     * @return the latest version name in string format
     */
    public String getCurrentVersionName() {
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

    /**
     * Gets the base path where object files are stored.
     *
     * @return path to objects directory
     */
    public String getObjectsFilePath() {
        return BASE_PATH + OBJECTS_PATH;
    }

    /**
     * Gets the base path where constraints files are stored.
     *
     * @return path to constraints directory
     */
    public String getConstraintsFilePath() {
        return getConstraintsFilePath(null);
    }

    /**
     * Gets the base path where table schema files are stored.
     *
     * @return path to table schema directory
     */
    public String getTableSchemaFilePath() {
        return getTableSchemaFilePath(null);
    }

    /**
     * Gets the file path of a specific constraints version.
     *
     * @param version the version name (e.g., "v1")
     * @return the file path for the requested constraints version
     */
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


    /**
     * Gets the file path of a specific table schema version.
     *
     * @param version the version name (e.g., "v1")
     * @return the file path for the requested table schema version
     */
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

    /**
     * Gets the latest file path for a specific table.
     *
     * @param tableName the name of the table
     * @return the latest file path for the table's data
     */
    public String getTableFilePath(String tableName) {
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");
        Map<String, Object> tableObjects = (Map<String, Object>) objects.get("tables");

        if (!tableObjects.containsKey(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " not found in metadata");
        }

        Map<String, Object> tableEntry = (Map<String, Object>) tableObjects.get(tableName);
        String fileName;
        List<String> incrementalList = (List<String>) tableEntry.get("incremental");

        if (incrementalList != null && !incrementalList.isEmpty()) {
            fileName = incrementalList.get(incrementalList.size() - 1);
        } else {
            fileName = (String) tableEntry.get("base");
        }

        return BASE_PATH + OBJECTS_PATH + OBJECTS_TABLES_PATH + "/" + tableName + "/" + fileName;
    }

    /**
     * Gets the base file path for a specific table.
     *
     * @param tableName the name of the table
     * @return the base file path for the table's data
     */
    public String getTableBaseFilePath(String tableName) {
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");
        Map<String, Object> tableObjects = (Map<String, Object>) objects.get("tables");

        if (!tableObjects.containsKey(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " not found in metadata");
        }

        Map<String, Object> tableEntry = (Map<String, Object>) tableObjects.get(tableName);
        String baseName = (String) tableEntry.get("base");

        if (baseName == null) {
            logger.error("No base file found for table " + tableName);
            return null;
        }

        return BASE_PATH + OBJECTS_PATH + OBJECTS_TABLES_PATH + "/" + tableName + "/" + baseName;
    }

    /**
     * Gets the current file path for a specific table.
     *
     * @param tableName the name of the table
     * @return the current file path for the table's data
     */
    public String getTableCurrentFilePath(String tableName) {
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");
        Map<String, Object> tableObjects = (Map<String, Object>) objects.get("tables");

        if (!tableObjects.containsKey(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " not found in metadata");
        }

        Map<String, Object> tableEntry = (Map<String, Object>) tableObjects.get(tableName);
        String currentName = (String) tableEntry.get("current");

        if (currentName == null) {
            logger.error("No current file found for table " + tableName);
            return null;
        }

        return BASE_PATH + OBJECTS_PATH + OBJECTS_TABLES_PATH + "/" + tableName + "/" + currentName;
    }

    /**
     * Gets all incremental file paths for a specific table.
     *
     * @param tableName the name of the table
     * @return list of incremental file paths for the table
     */
    public List<String> getIncrementalFilePath(String tableName) {
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");
        Map<String, Object> tableObjects = (Map<String, Object>) objects.get("tables");

        if (!tableObjects.containsKey(tableName)) {
            return new ArrayList<>();
        }

        Map<String, Object> tableEntry = (Map<String, Object>) tableObjects.get(tableName);
        List<String> incrementalFileNames = (List<String>) tableEntry.getOrDefault("incremental", new ArrayList<>());

        List<String> incrementalFilePaths = new ArrayList<>();
        for (String fileName : incrementalFileNames) {
            String fullPath = BASE_PATH + OBJECTS_PATH + OBJECTS_TABLES_PATH + "/" + tableName + "/" + fileName;
            incrementalFilePaths.add(fullPath);
        }

        return incrementalFilePaths;
    }

    /**
     * Retrieves the list of table names stored in metadata.
     *
     * @return list of table names
     */
    public List<String> getTableNames() {
        Map<String, Object> objects = (Map<String, Object>) metadata.get("objects");
        Map<String, Object> tableObjects = (Map<String, Object>) objects.get("tables");

        return new ArrayList<>(tableObjects.keySet());
    }
}
