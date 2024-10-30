package org.example;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "dcopy", description = "Database copy tool")
public class main implements Callable<Integer> {

    @CommandLine.Option(names= {"-src", "--source"}, description = "Source database URL", required = true)
    private String source;

    @CommandLine.Option(names = {"-dest", "--destination"}, description = "Destination database URL", required = true)
    private String destination;

    public static void main(String[] args) {
        //int exitCode = new CommandLine(new main()).execute(args);
        //System.exit(exitCode);



    }
    @Override
    public Integer call() throws Exception {
        return 0;
    }
}
