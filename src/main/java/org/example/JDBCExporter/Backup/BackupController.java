package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.FileWriter;
import org.example.JDBCExporter.MetaDataController;
import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class BackupController {
    // db connection constants

    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;

    private final CreateBackup createBackup;
    private final RestoreBackup restoreBackup;

    /**
     * Constructor for BackupController is initialized
     * @param url the URL of the conncetion to the database only with the database name e.g: jdbc:postgresql://localhost:5432/databack
     * @param user the username which has permission for given database
     * @param password the password for the given user
     * @throws SQLException
     */
    public BackupController(String url, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, user, password);
        this.logger = new Logger();
        this.fileWriter = new FileWriter();
        this.createBackup = new CreateBackup(connection, logger, fileWriter);
        this.restoreBackup = new RestoreBackup(connection, logger);
    }

    /**
     * Performs the Backup and stores the backup data in the parent directory of the .jar file
     * The structure of the Backup data can be seen in the gitbook of the project
     * @throws SQLException
     * @throws IOException
     */
    public void performBackup() throws SQLException, IOException {
        createBackup.performBackup();
    }

    /**
     * Restores the Backup of the given version.
     * @param version in form "v<VERSION_NUMBER>"
     * @throws SQLException
     * @throws IOException
     */
    public void restoreBackupFromVersion(String version) throws SQLException, IOException {
        restoreBackup.performRestore(version);
    }

    /**
     * Restores the backup of the most current version
     * @throws SQLException
     * @throws IOException
     */
    public void restoreLastBackup() throws SQLException, IOException {
        restoreBackup.performRestore(MetaDataController.getInstance().getCurrentVersionName());
    }

    /**
     * Only for test uses
     * @param args
     * @throws SQLException
     * @throws IOException
     */
    public static void main(String[] args) throws SQLException, IOException {
        BackupController backupController = new BackupController("jdbc:postgresql://localhost:5432/databack", "benutzer", "passwort");
        //backupController.doBackup();
        //backupController.restoreLastBackup();
        backupController.restoreBackupFromVersion("v1");
    }
}
