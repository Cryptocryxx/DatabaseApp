package hdm.DataBack.JDBCExporter;

import hdm.DataBack.Logger.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static hdm.DataBack.JDBCExporter.DataTypeMapper.mapDataType;

public class SchemaExporter {
    Logger logger = new Logger();

    private final Connection connection;
    private final StringBuilder tableScript;
    private final StringBuilder constraintsScript;

    /**
     * Constructor for SchemaExporter that initializes a database connection.
     *
     * @param connection the database connection
     */
    public SchemaExporter(Connection connection) {
        this.connection = connection;
        this.tableScript = new StringBuilder();
        this.constraintsScript = new StringBuilder();
    }

    /**
     * Generates SQL scripts for creating tables in the database.
     *
     * @return a string containing SQL CREATE TABLE statements
     * @throws SQLException if a database access error occurs
     */
    public String generateTableScript() throws SQLException {
        logger.info("Start generating table scripts...");
        List<String> tableNames = getTableNames();

        for(String tableName : tableNames){
            logger.info(String.format("Generating CREATE TABLE script for table: %s", tableName));
            tableScript.append(String.format("CREATE TABLE \"%s\" (\n", tableName));
            List<String> columnDefinitions = getColumnDefinitions(tableName);

            tableScript.append(String.join(",\n", columnDefinitions)).append("\n);\n\n");
        }
        logger.info("Table script generation completed successfully.");
        return tableScript.toString();
    }

    /**
     * Generates SQL scripts for adding constraints (primary keys, foreign keys, unique, check constraints).
     *
     * @return a string containing SQL ALTER TABLE statements for constraints
     * @throws SQLException if a database access error occurs
     */
    public String generateConstraintsScript() throws SQLException {
        logger.info("Start generating constraints scripts...");
        List<String> tableNames = getTableNames();

        for(String tableName: tableNames) {
            logger.info(String.format("Generating PrimaryKeys for table: %s", tableName));
            getPrimaryKeys(tableName);
        }
        for(String tableName: tableNames) {
            logger.info(String.format("Generating constraints for table: %s", tableName));
            getForeignKeys(tableName);
            getUniqueConstraints(tableName);
            getCheckConstraints(tableName);
        }
        logger.info("Constraints script generation completed successfully.");
        return constraintsScript.toString();
    }

    /**
     * Retrieves the list of table names from the database.
     *
     * @return a list of table names
     * @throws SQLException if a database access error occurs
     */
    private List<String> getTableNames() throws SQLException {
        logger.info("Fetching table names from the database...");
        String query = String.format("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'""");

        List<String> tableNames = new ArrayList<>();

        try(Statement statement = connection.createStatement(); ResultSet tables = statement.executeQuery(query)){
            while (tables.next()){
                tableNames.add(tables.getString("table_name"));
            }
        }
        logger.info("Successfully fetched table names.");
        return tableNames;
    }

    /**
     * Retrieves column definitions for a given table.
     *
     * @param tableName the name of the table
     * @return a list of column definitions in SQL format
     * @throws SQLException if a database access error occurs
     */
    private List<String> getColumnDefinitions(String tableName) throws SQLException {
        logger.info(String.format("Fetching column definitions for table: %s", tableName));
        String query = String.format("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = '%s'""", tableName);

        List<String> columnDefinitions = new ArrayList<>();

        try(Statement statement = connection.createStatement(); ResultSet columns = statement.executeQuery(query)){
            while (columns.next()){
                String columnName = columns.getString("column_name");
                String dataType = columns.getString("data_type");
                String columnDefault = columns.getString("column_default");

                boolean isSerial = columnDefault != null && columnDefault.startsWith("nextval(");

                String columnDefinition;
                if (isSerial) {
                    columnDefinition = String.format("\"%s\" SERIAL", columnName);
                } else {
                    String defaultValue = (columnDefault != null) ? String.format(" DEFAULT %s", columnDefault) : "";
                    columnDefinition = String.format("\"%s\" %s%s", columnName, mapDataType(dataType), defaultValue);
                }

                columnDefinitions.add(columnDefinition);

                if(columns.getString("is_nullable").equals("NO")) {
                    constraintsScript.append(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL;\n", tableName, columnName));
                }
            }
            logger.info(String.format("Successfully fetched column definitions for table: %s", tableName));
        }

        return columnDefinitions;
    }

    /**
     * Retrieves primary key constraints for a given table and appends them to the constraints script.
     *
     * @param tableName the name of the table
     * @throws SQLException if a database access error occurs
     */
    private void getPrimaryKeys(String tableName) throws SQLException {
        String query = String.format("""
        SELECT kcu.column_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name
        WHERE kcu.table_name = '%s' AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position""", tableName);

        logger.info(String.format("Fetching primary keys for table: %s", tableName));

        try (Statement statement = connection.createStatement();
             ResultSet primaryKeys = statement.executeQuery(query)) {

            StringBuilder primaryKeyColumns = new StringBuilder();

            while (primaryKeys.next()) {
                if (primaryKeyColumns.length() > 0) {
                    primaryKeyColumns.append(", ");
                }
                primaryKeyColumns.append("\"" + primaryKeys.getString("column_name") + "\"");
            }

            if (primaryKeyColumns.length() > 0) {
                constraintsScript.append(String.format("ALTER TABLE \"%s\" ADD PRIMARY KEY (%s);\n",
                        tableName, primaryKeyColumns.toString()));
            }
        }
    }

    /**
     * Retrieves foreign key constraints for a given table and appends them to the constraints script.
     *
     * @param tableName the name of the table
     * @throws SQLException if a database access error occurs
     */
    private void getForeignKeys(String tableName) throws SQLException {
        String query = String.format("""
            SELECT kcu.constraint_name, kcu.column_name, ccu.table_name AS referenced_table, ccu.column_name AS referenced_column 
            FROM information_schema.key_column_usage kcu 
            JOIN information_schema.table_constraints tc ON kcu.constraint_name = tc.constraint_name 
            JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name 
            WHERE kcu.table_name = '%s' AND tc.constraint_type = 'FOREIGN KEY'""", tableName);

        logger.info(String.format("Fetching foreign keys for table: %s", tableName));
        try (Statement statement = connection.createStatement(); ResultSet foreignKeys = statement.executeQuery(query)) {
            while (foreignKeys.next()) {
                constraintsScript.append(String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" FOREIGN KEY (\"%s\") REFERENCES \"%s\"(\"%s\");\n",
                        tableName,
                        foreignKeys.getString("constraint_name"),
                        foreignKeys.getString("column_name"),
                        foreignKeys.getString("referenced_table"),
                        foreignKeys.getString("referenced_column")));
            }
        }
    }

    /**
     * Retrieves unique constraints for a given table and appends them to the constraints script.
     *
     * @param tableName the name of the table
     * @throws SQLException if a database access error occurs
     */
    private void getUniqueConstraints(String tableName) throws SQLException {
        String query = String.format("""
            SELECT tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '%s' AND tc.constraint_type = 'UNIQUE'""", tableName);

        logger.info(String.format("Fetching unique constraints for table: %s", tableName));
        try (Statement statement = connection.createStatement(); ResultSet uniqueConstraints = statement.executeQuery(query)) {
            while (uniqueConstraints.next()) {
                constraintsScript.append(String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" UNIQUE (\"%s\");\n",
                        tableName,
                        uniqueConstraints.getString("constraint_name"),
                        uniqueConstraints.getString("column_name")));
            }
        }
    }

    /**
     * Retrieves check constraints for a given table and appends them to the constraints script.
     *
     * @param tableName the name of the table
     * @throws SQLException if a database access error occurs
     */
    private void getCheckConstraints(String tableName) throws SQLException {
        String checkQuery = String.format("""
            SELECT cc.constraint_name, cc.check_clause
            FROM information_schema.check_constraints cc
            JOIN information_schema.constraint_table_usage ctu ON cc.constraint_name = ctu.constraint_name
            WHERE ctu.table_name = '%s'""", tableName);

        logger.info(String.format("Fetching check constraints for table: %s", tableName));
        try (Statement statement = connection.createStatement(); ResultSet checkConstraints = statement.executeQuery(checkQuery)) {
            while (checkConstraints.next()) {
                constraintsScript.append(String.format("ALTER TABLE \"%s\" ADD CONSTRAINT \"%s\" CHECK (%s);\n",
                        tableName,
                        checkConstraints.getString("constraint_name"),
                        checkConstraints.getString("check_clause")));
            }
        }
    }
}
