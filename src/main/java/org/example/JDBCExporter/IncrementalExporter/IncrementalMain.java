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
            Connection connection = DriverManager.getConnection(url, user, password);
            ObjectExporter objectExporter = new ObjectExporter(connection, "./src/main/java/org/example/TestData/Objects");

            for(Map.Entry<String, String> entry: dataFiles.entrySet()) {
                String tableName = entry.getKey();
                String oldFilePath = entry.getValue();
                String newFilePath = oldFilePath.replace(oldVersion, newVersion);

                List<Map<String, Object>> oldData = Arrays.asList(objectMapper.readValue(new File(oldFilePath), Map[].class));
                List<Map<String, Object>> newData;

                newData = objectExporter.fetchDataFromTable(tableName);
                fileWriter.writeJSONFile(newFilePath, newData);

                Map<String, Object> changes = compareData(oldData, newData);
                fileWriter.writeJSONFile(newFilePath, Collections.singletonList(changes));
                logger.info(String.format("Incremental changes for table '%s' saved to %s", tableName, newFilePath));

            }

        }catch (Exception e){
            logger.error(e.getMessage());
        }

    }

    public static void main(String[] args) {
        String metaFilePath = "./src/main/java/org/example/TestData/metadata.json";
        String oldVersion = "v1";
        String newVersion = "v2";
        try{
            IncrementalMain main = new IncrementalMain();
            main.processTables(metaFilePath, oldVersion, newVersion);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
