package org.example.JDBCExporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.Logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileWriter {
    Logger logger = new Logger();
    ObjectMapper objectMapper = new ObjectMapper();

    public void writeFile(String filePath, String content) throws IOException {
        try(java.io.FileWriter writer = new java.io.FileWriter(filePath)){
            writer.write(content);
            logger.info(String.format("File exported successfully: %s", filePath));
        }catch (IOException e){
            logger.error(String.format("Error writing file: %s: %s", filePath, e.getMessage()));
        }
    }

    public void writeJSONFile(String filePath, List<Map<String, Object>> data) throws IOException {
        try {
            List<Map<String, Object>> indexedData = new ArrayList<>();

            for(Map<String, Object> entry : data){
                Map<String, Object> indexedEntry = new HashMap<>();
                indexedEntry.put("index", UUID.randomUUID().toString());
                indexedEntry.put("object", entry);

                indexedData.add(indexedEntry);
            }
            File jsonFile = new File(filePath);
            objectMapper.writeValue(jsonFile, indexedData);
            logger.info(String.format("Data exported to JSON successfully: %s", jsonFile.getName()));
        } catch (IOException e) {
            logger.error(String.format("Error writing JSON file: %s: %s", filePath, e.getMessage()));
        }
    }

    public void writeJSONFile(String filePath, Map<String, Object> data) throws IOException {
        try {
            File jsonFile = new File(filePath);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, data);
            logger.info(String.format("Data exported to JSON successfully: %s", jsonFile.getName()));
        } catch (IOException e) {
            logger.error(String.format("Error writing JSON file: %s: %s", filePath, e.getMessage()));
        }
    }

    public void createDirectory(String filePath){
        Path path = Paths.get(filePath);

        try{
            Files.createDirectories(path);
            logger.info(String.format("Directory created successfully: %s", filePath));
        } catch (IOException e) {
            logger.error(String.format("Error creating directory %s: %s", filePath, e.getMessage()));
        }

    }
}
