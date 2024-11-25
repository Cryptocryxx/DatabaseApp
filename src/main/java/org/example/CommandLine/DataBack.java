package org.example.CommandLine;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "databack", mixinStandardHelpOptions = true, version = "DataBack 1.0",
        description = "Backup and restore PostgreSQL databases with ease.")
public class DataBack implements Callable<Integer> {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DataBack())
                .addSubcommand(new BackupCommand())
                .addSubcommand(new RestoreCommand())
                .execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    @Command(name = "backup", mixinStandardHelpOptions = true,
            description = "Create a backup of a PostgreSQL database.")
    static class BackupCommand implements Callable<Integer> {

        @Option(names = {"--db-name"}, required = true, description = "Name of the database to back up.")
        private String dbName;

        @Option(names = {"--output"}, required = true, description = "File path where the backup will be stored.")
        private String outputPath;

        @Option(names = {"--incremental"}, description = "Perform an incremental backup to save only the changes.")
        private boolean incremental;

        @Override
        public Integer call() {
            System.out.printf("Backing up database '%s' to '%s'%s%n",
                    dbName, outputPath, incremental ? " (incremental)" : "");
            return 0;
        }
    }

    @Command(name = "restore", mixinStandardHelpOptions = true,
            description = "Restore a PostgreSQL database from a backup file.")
    static class RestoreCommand implements Callable<Integer> {

        @Option(names = {"--db-name"}, required = true, description = "Name of the database to restore.")
        private String dbName;

        @Option(names = {"--input"}, required = true, description = "Path to the backup file.")
        private String inputPath;

        @Override
        public Integer call() {
            System.out.printf("Restoring database '%s' from '%s'%n", dbName, inputPath);
            return 0;
        }
    }
}
