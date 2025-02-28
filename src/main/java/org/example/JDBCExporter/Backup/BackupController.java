package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.FileWriter;
import org.example.JDBCExporter.MetaDataController;
import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class BackupController {
    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;

    private final CreateBackup createBackup;
    private final RestoreBackup restoreBackup;

    /**
     * Constructor for BackupController that initializes database connection and backup operations.
     *
     * @param url the database connection URL (e.g., jdbc:postgresql://localhost:5432/databack)
     * @param user the username with access to the database
     * @param password the password for the database user
     * @throws SQLException if a database access error occurs
     */
    public BackupController(String url, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, user, password);
        this.logger = new Logger();
        this.fileWriter = new FileWriter();
        this.createBackup = new CreateBackup(connection, logger, fileWriter);
        this.restoreBackup = new RestoreBackup(connection, logger);
    }

    /**
     * Performs a full backup of the database.
     * - Exports table data as JSON files.
     * - Saves table schema and constraints as SQL scripts.
     * - Stores only incremental changes if a previous backup exists.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while writing backup files
     */
    public void performBackup() throws SQLException, IOException {
        createBackup.performBackup();
    }

    /**
     * Restores a specific backup version.
     *
     * @param version the version identifier in the format "v<VERSION_NUMBER>"
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while reading backup files
     */
    public void restoreBackupFromVersion(String version) throws SQLException, IOException {
        restoreBackup.performRestore(version);
    }

    /**
     * Restores the most recent backup version.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while reading backup files
     */
    public void restoreLastBackup() throws SQLException, IOException {
        restoreBackup.performRestore(MetaDataController.getInstance().getCurrentVersionName());
    }

    /**
     * Main method for testing the BackupController functionality.
     *
     * @param args command-line arguments (not used)
     * @throws SQLException if a database access error occurs
     * @throws IOException if an error occurs while handling backups
     */
    public static void main(String[] args) throws SQLException, IOException {
        BackupController backupController = new BackupController("jdbc:postgresql://localhost:5432/databack", "benutzer", "passwort");
        //backupController.doBackup();
        //backupController.restoreLastBackup();
        backupController.restoreBackupFromVersion("v1");
    }
}
