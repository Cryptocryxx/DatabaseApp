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

    public Map<String, Object> compareData(List<Map<String, Object>> oldData, List<Map<String, Object>> newData){
        logger.info("OldData: " + oldData.toString());
        logger.info("NewData: " + newData.toString());
        List<Map<String, Object>> added = new ArrayList<>(newData);
        List<Map<String, Object>> deleted = new ArrayList<>();

        for(Map<String, Object> oldRow : oldData){
            boolean matched = false;
            Iterator<Map<String, Object>> iterator = added.iterator();

            while(iterator.hasNext()){
                Map<String, Object> newRow = iterator.next();
                if(newRow.equals(oldRow)){
                    matched = true;
                    iterator.remove();
                    break;
                }
            }
            if(!matched){
                deleted.add(oldRow);
            }
        }
        // Create changes map
        Map<String, Object> changes = new HashMap<>();
        changes.put("added", added);
        changes.put("deleted", deleted);

        return changes;
    }

    public void processTables(String metaFilePath, String oldVersion, String newVersion) throws IOException {

        //Loading metadata
        Map<String, Object> metaData = objectMapper.readValue(new File(metaFilePath), Map.class);
        Map<String, String> dataFiles = (Map<String, String>) metaData.get("data_files");

        try{
            //Connection connection = DriverManager.getConnection(url, user, password);
            //ObjectExporter objectExporter = new ObjectExporter(connection, "./src/main/java/org/example/TestData/Objects");

            for(Map.Entry<String, String> entry: dataFiles.entrySet()) {
                String tableName = entry.getKey();
                String newFilePath = entry.getValue();
                String oldFilePath = newFilePath.replace(newVersion, oldVersion);

                List<Map<String, Object>> oldData = Arrays.asList(objectMapper.readValue(new File(oldFilePath), Map[].class));
                List<Map<String, Object>> newData = Arrays.asList(objectMapper.readValue(new File(newFilePath), Map[].class));
                //fileWriter.writeJSONFile(newFilePath, newData);
                String incrementalFilePath = newFilePath.replace(newVersion, "incremental_" + newVersion);

                Map<String, Object> changes = compareData(oldData, newData);
                fileWriter.writeJSONFile(incrementalFilePath, Collections.singletonList(changes));
                logger.info(String.format("Incremental changes for table '%s' saved to %s", tableName, oldFilePath));

                if (new File(newFilePath).delete()) {
                    logger.info(String.format("File '%s' deleted successfully after processing.", newFilePath));
                } else {
                    logger.warn(String.format("File '%s' could not be deleted.", newFilePath));
                }
            }

        }catch (Exception e){
            logger.error(e.getMessage());
        }

    }

}
