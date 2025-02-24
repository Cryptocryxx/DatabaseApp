package org.example.JDBCExporter.Backup;

import org.example.JDBCExporter.FileWriter;
import org.example.Logger.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class BackupController {
    // db connection constants
    private final String URL = "jdbc:postgresql://localhost:5432/my_database";
    private final String USER = "user";
    private final String PASSWORD = "password";

    private final Connection connection;
    private final Logger logger;
    private final FileWriter fileWriter;

    private final CreateBackupHelper createBackupHelper;
    private final RestoreBackupHelper restoreBackupHelper;

    public BackupController() throws SQLException {
        this.connection = DriverManager.getConnection(URL, USER, PASSWORD);
        this.logger = new Logger();
        this.fileWriter = new FileWriter();
        this.createBackupHelper = new CreateBackupHelper(connection, logger, fileWriter);
        this.restoreBackupHelper = new RestoreBackupHelper(connection, logger, fileWriter);
    }

    public void doBackup() throws SQLException, IOException {
        createBackupHelper.performBackup();
    }

    public void restoreBackup() throws SQLException, IOException {
        restoreBackupHelper.performRestore();
    }

    public static void main(String[] args) throws SQLException, IOException {
        BackupController backupController = new BackupController();
        backupController.doBackup();
    }
}
