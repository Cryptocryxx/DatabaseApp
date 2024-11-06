package org.example.JDBCExporter;

import java.io.FileWriter;
import java.io.IOException;

public class FileExporter {
    public void exportToFile(String filePath, String content) throws IOException {
        try(FileWriter writer = new FileWriter(filePath)){
            writer.write(content);
            System.out.printf("File exported successfully: %s%n", filePath);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
