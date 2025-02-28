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

    private final CreateBackup createBackup;
    private final RestoreBackup restoreBackup;

    public BackupController(String url, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, user, password);
        this.logger = new Logger();
        this.fileWriter = new FileWriter();
        this.createBackup = new CreateBackup(connection, logger, fileWriter);
        this.restoreBackup = new RestoreBackup(connection, logger);
    }

    public void doBackup() throws SQLException, IOException {
        createBackup.performBackup();
    }

    public void restoreBackupFromVersion(String version, String password) throws SQLException, IOException {
        restoreBackup.performRestore(version, password);
    }
    public void restoreLastBackup(String password) throws SQLException, IOException {
        restoreBackup.performRestore(MetaDataController.getInstance().getCurrentVersionName(), password);
    }

    public static void main(String[] args) throws SQLException, IOException {
        BackupController backupController = new BackupController("jdbc:postgresql://localhost:5432/my_database", "user", "password");
        backupController.doBackup();
        //backupController.restoreLastBackup("passwort");
        //backupController.restoreBackupFromVersion("v6", "passwort");
    }
}
