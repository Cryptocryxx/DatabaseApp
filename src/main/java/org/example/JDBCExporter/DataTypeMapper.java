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

    /**
     * Maps a database-specific data type to a standardized SQL data type.
     * If no mapping is found, returns the original data type.
     *
     * @param dataType the database-specific data type
     * @return the standardized SQL data type or the original data type if no mapping exists
     */
    public static String mapDataType(String dataType) {
        return dataTypeMappings.getOrDefault(dataType, dataType);
    }
}
