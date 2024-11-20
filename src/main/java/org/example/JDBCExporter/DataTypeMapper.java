package org.example.JDBCExporter;

import java.util.HashMap;
import java.util.Map;

public class DataTypeMapper {
    private static final Map<String, String> dataTypeMappings = new HashMap<>();

    static {
        dataTypeMappings.put("integer", "INT");
        dataTypeMappings.put("character varying", "VARCHAR(255)");
        dataTypeMappings.put("timestamp without time zone", "TIMESTAMP");
    }

    public static String mapDataType(String dataType) {
        return dataTypeMappings.getOrDefault(dataType, dataType);
    }
}
