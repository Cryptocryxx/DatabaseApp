package org.example.JDBCExporter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.example.JDBCExporter.DataTypeMapper.mapDataType;

public class SchemaExporter {
    private final Connection connection;
    private final StringBuilder tableScript;
    private final StringBuilder constraintsScript;

    public SchemaExporter(Connection connection) {
        this.connection = connection;
        this.tableScript = new StringBuilder();
        this.constraintsScript = new StringBuilder();
    }

    public String generateTableScript() throws SQLException {
        List<String> tableNames = getTableNames();

        for(String tableName : tableNames){
            tableScript.append(String.format("CREATE TABLE %s (\n", tableName));
            List<String> columnDefinitions = getColumnDefinitions(tableName);

            tableScript.append(String.join(",\n", columnDefinitions)).append("\n);\n\n");
        }
        return tableScript.toString();
    }

    public String generateConstraintsScript() throws SQLException {
        List<String> tableNames = getTableNames();

        for(String tableName: tableNames) {
            getPrimaryKeys(tableName);
            getForeignKeys(tableName);
            getUniqueConstraints(tableName);
            getCheckConstraints(tableName);
        }

        return constraintsScript.toString();
    }

    private List<String> getTableNames() throws SQLException {
        String query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'""";

        List<String> tableNames = new ArrayList<>();

        try(Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)){
            while (tables.next()){
                tableNames.add(tables.getString("table_name"));
            }
        }

        return tableNames;
    }

    private List<String> getColumnDefinitions(String tableName) throws SQLException {
        String query = String.format("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '%s'""", tableName);

        List<String> columnDefinitions = new ArrayList<>();

        try(Statement statement = connection.createStatement(); ResultSet columns = statement.executeQuery(query)){
            while (columns.next()){
                String columnName = columns.getString("column_name");
                String dataType = columns.getString("data_type");
                String defaultValue = (columns.getString("column_default") != null && !columns.getString("column_default").contains("nextval"))
                        ? String.format(" DEFAULT %s", columns.getString("column_default")) : "";
                columnDefinitions.add(String.format("%s %s%s", columnName, mapDataType(dataType), defaultValue));

                if(columns.getString("is_nullable").equals("NO")) {
                    constraintsScript.append(String.format("ALTER TABLE %s ALTER COLUMN %S SET NOT NULL;\n", tableName, columnName));
                }
            }
        }
        
        return columnDefinitions;
    }

    private void getPrimaryKeys(String tableName) throws SQLException {
        String query = String.format("""
            SELECT kcu.column_name
            FROM information_schema.key_column_usage kcu
            JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
            WHERE kcu.table_name = '%s' AND tc.constraint_type = 'PRIMARY KEY'""", tableName);

        try (Statement statement = connection.createStatement(); ResultSet primaryKeys = statement.executeQuery(query)) {
            if (primaryKeys.next()) {
                constraintsScript.append(String.format("ALTER TABLE %s ADD PRIMARY KEY (%s);\n", tableName, primaryKeys.getString("column_name")));
            }
        }
    }

    private void getForeignKeys(String tableName) throws SQLException {
        String query = String.format("""
            SELECT kcu.constraint_name, kcu.column_name, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column 
            FROM information_schema.key_column_usage kcu 
            JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name 
            JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name 
            WHERE kcu.table_name = '%s' AND tc.constraint_type = 'FOREIGN KEY'""", tableName);

        try (Statement statement = connection.createStatement(); ResultSet foreignKeys = statement.executeQuery(query)) {
            while (foreignKeys.next()) {
                constraintsScript.append(String.format("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s);\n",
                        tableName,
                        foreignKeys.getString("constraint_name"),
                        foreignKeys.getString("column_name"),
                        foreignKeys.getString("referenced_table"),
                        foreignKeys.getString("referenced_column")));
            }
        }
    }

    private void getUniqueConstraints(String tableName) throws SQLException {
        String query = String.format("""
            SELECT tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '%s' AND tc.constraint_type = 'UNIQUE'""", tableName);

        try (Statement statement = connection.createStatement(); ResultSet uniqueConstraints = statement.executeQuery(query)) {
            while (uniqueConstraints.next()) {
                constraintsScript.append(String.format("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s);\n",
                        tableName,
                        uniqueConstraints.getString("constraint_name"),
                        uniqueConstraints.getString("column_name")));
            }
        }
    }

    private void getCheckConstraints(String tableName) throws SQLException {
        String checkQuery = String.format("""
            SELECT cc.constraint_name, cc.check_clause
            FROM information_schema.check_constraints cc
            JOIN information_schema.constraint_table_usage ctu ON cc.constraint_name = ctu.constraint_name
            WHERE ctu.table_name = '%s'""", tableName);

        try (Statement statement = connection.createStatement(); ResultSet checkConstraints = statement.executeQuery(checkQuery)) {
            while (checkConstraints.next()) {
                constraintsScript.append(String.format("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);\n",
                        tableName,
                        checkConstraints.getString("constraint_name"),
                        checkConstraints.getString("check_clause")));
            }
        }
    }
}
