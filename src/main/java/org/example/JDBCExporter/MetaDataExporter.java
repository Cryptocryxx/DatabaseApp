package org.example.JDBCExporter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetaDataExporter {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String metaFilePath = "./src/main/java/org/example/TestData/metadata.json";
    static Logger logger = new Logger();
    public String getNextVersion(){
        File metaFile = new File(metaFilePath);
        if(!metaFile.exists()){
            logger.warn("No metaFile exists.");
            return "v1";
        }
        try{
            JsonNode root = objectMapper.readTree(metaFile);
            String currentVersion = root.get("version").asText();
            if(currentVersion.startsWith("v")){
                int versionNumber = Integer.parseInt(currentVersion.substring(1));
                logger.info("Current version is " + currentVersion);
                logger.info("New version is " + versionNumber);
                return "v"+ (versionNumber + 1);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        logger.warn("No metaFile exists, returns default version.");
        return "v1";
    }

    public void exportMetaData(String version, String tableScriptFile, String constraintsScriptFile, Map<String, String> dataFiles) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("version", version);
        metadata.put("schema_files", Map.of(
                "table_script", tableScriptFile,
                "constraints_script", constraintsScriptFile
        ));
        metadata.put("data_files", dataFiles);

        try {
            File metaFile = new File("./src/main/java/org/example/TestData/metadata.json");
            objectMapper.writeValue(metaFile, metadata);
            System.out.println("Metadata successfully written to " + metaFile.getName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
