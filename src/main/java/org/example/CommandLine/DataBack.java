package org.example.CommandLine;

import org.example.JDBCExporter.Backup.BackupController;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

@Command(name = "databack", mixinStandardHelpOptions = true, version = "DataBack 1.0",
        description = "Backup and restore PostgreSQL databases with ease.")
public class DataBack implements Callable<Integer> {

    /**
     * Main entry point for the DataBack command-line tool.
     * - Supports backup and restore operations for PostgreSQL databases.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int exitCode = new CommandLine(new DataBack())
                .addSubcommand(new BackupCommand())
                .addSubcommand(new RestoreCommand())
                .execute(args);
        System.exit(exitCode);
    }

    /**
     * Default command behavior that displays the usage information.
     *
     * @return exit code 0
     */
    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    /**
     * Command to create an incremental backup of a PostgreSQL database.
     * - Requires database URL, user, and password.
     */
    @Command(name = "backup", mixinStandardHelpOptions = true,
            description = "Create an incremental backup of a PostgreSQL database.")
    static class BackupCommand implements Callable<Integer> {

        @Option(names = {"--db-url"}, required = true, description = "url of the postgres database without user and password")
        private String url;

        @Option(names = {"--user"}, required = true, description = "Name of the user with access to the database")
        private String user;

        @Option(names = {"--password"}, required = true, description = "the password for the user")
        private String password;

        /**
         * Executes the backup operation.
         *
         * @return exit code 0
         */
        @Override
        public Integer call() {
            System.out.printf("Backing up database '%s'",
                    url);
            try {
                BackupController backupController = new BackupController(url, user, password);
                backupController.performBackup();
            } catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
            return 0;
        }
    }

    /**
     * Command to restore a PostgreSQL database from a backup.
     * - Requires database URL, user, and password.
     * - An optional version parameter allows restoring a specific backup version.
     */
    @Command(name = "restore", mixinStandardHelpOptions = true,
            description = "Restore a PostgreSQL database from a backup file.")
    static class RestoreCommand implements Callable<Integer> {

        @Option(names = {"--db-url"}, required = true, description = "url of the postgres database without user and password")
        private String url;

        @Option(names = {"--user"}, required = true, description = "Name of the user with access to the database")
        private String user;

        @Option(names = {"--password"}, required = true, description = "the password for the user")
        private String password;

        @Option(names = {"--version"}, required = false, description = "version name (in Form \"v<VERSION_NUMBER>\"")
        private String version;

        /**
         * Executes the restore operation.
         * - If no version is provided, restores the most recent backup.
         * - If a version is specified, restores the given backup version.
         *
         * @return exit code 0
         */
        @Override
        public Integer call() {
            System.out.printf("Restoring database '%s' from '%s'%n", url, version);
            try {
                BackupController backupController = new BackupController(url, user, password);
                if (version == null) {
                    backupController.restoreLastBackup();
                }else {
                    backupController.restoreBackupFromVersion(version);
                }

            }catch (SQLException | IOException e) {
                throw new RuntimeException(e);
            }
            return 0;
        }
    }
}
