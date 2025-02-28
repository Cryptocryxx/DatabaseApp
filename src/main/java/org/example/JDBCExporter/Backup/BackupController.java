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
    /*
    private final String URL = "jdbc:postgresql://localhost:5432/databack";
    private final String USER = "benutzer";
    private final String PASSWORD = "passwort";
    */

    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;

    private final CreateBackupHelper createBackupHelper;
    private final RestoreBackupHelper restoreBackupHelper;

    public BackupController(String url, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, user, password);
        this.logger = new Logger();
        this.fileWriter = new FileWriter();
        this.createBackupHelper = new CreateBackupHelper(connection, logger, fileWriter);
        this.restoreBackupHelper = new RestoreBackupHelper(connection, logger);
    }

    public void doBackup() throws SQLException, IOException {
        createBackupHelper.performBackup();
    }

    public void restoreBackupFromVersion(String version, String password) throws SQLException, IOException {
        restoreBackupHelper.performRestore(version, password);
    }
    public void restoreLastBackup(String password) throws SQLException, IOException {
        restoreBackupHelper.performRestore(MetaDataController.getInstance().getCurrentVersionName(), password);
    }

    public static void main(String[] args) throws SQLException, IOException {
        BackupController backupController = new BackupController("jdbc:postgresql://localhost:5432/databack", "benutzer", "passwort");
        backupController.doBackup();
        //backupController.restoreLastBackup();
        //backupController.restoreBackupFromVersion("v6", "passwort");
    }
}
