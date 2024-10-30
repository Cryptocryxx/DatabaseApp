package org.example;
import picocli.CommandLine;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "dcopy", description = "Database copy tool")
public class main implements Callable<Integer> {

   // @CommandLine.Option(names= {"-src", "--source"}, description = "Source database URL", required = true)
    private String source = "jdbc:postgresql://localhost:5432/my_database";

    @CommandLine.Option(names = {"-dest", "--destination"}, description = "Destination database URL", required = true)
    private String destination;

    public static void main(String[] args) {
        //int exitCode = new CommandLine(new main()).execute(args);
        //System.exit(exitCode);



    }
    private String username = "user";
    private String password = "password";
    @Override
    public Integer call() throws Exception {
        try (Connection conn = DriverManager.getConnection(source, username, password)) {
            System.out.println("Connected to the database!");

            // Eine einfache Abfrage, um alle Tabellen auszugeben
            String query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'";
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
                System.out.println("Tables in the database:");
                while (rs.next()) {
                    System.out.println(rs.getString("table_name"));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 1; // Fehlercode zurückgeben
        }
        return 0;
    }
}
