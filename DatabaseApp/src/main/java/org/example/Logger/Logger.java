package org.example.Logger;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/*

The logger is divided into four different log levels, INFO, DEBUG, WARN and ERROR. There are three different ways
to use the constructor:
    1. do not include a variable // Logger logger = new Logger();
        - here every single message is logged in the terminal.
    2. a LogLevel is passed // Logger logger = new Logger(Logger.LogLevel.DEBUG);
        - everything that is at the LogLevel or higher is logged in the terminal.
    3. a log level and a file path are specified // Logger logger = new Logger(Logger.LogLevel.DEBUG, “application.log”);
        - Everything in the terminal and in the file that is at the LogLevel or higher is logged here.

 */


public class Logger {

    private static final String RESET = "\u001B[0m";
    private static final String INFO_COLOR = "\u001B[32m";
    private static final String DEBUG_COLOR = "\u001B[34m";
    private static final String WARN_COLOR = "\u001B[33m";
    private static final String ERROR_COLOR = "\u001B[31m";


    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private LogLevel logLevel;
    private String logFilePath;

    public enum LogLevel {
        INFO, DEBUG, WARN, ERROR
    }

    public Logger() {
        this(LogLevel.INFO, null);
    }

    public Logger(LogLevel logLevel) {
        this(logLevel, null);
    }

    public Logger(LogLevel logLevel, String logFilePath) {
        this.logLevel = logLevel != null ? logLevel : LogLevel.INFO;
        this.logFilePath = logFilePath;
    }

    private void log(LogLevel level, String message) {
        if (level.ordinal() >= logLevel.ordinal()) {
            String timestamp = LocalDateTime.now().format(formatter);
            String color = getColor(level);
            String logMessage = String.format("%s%s [%s]: %s%s", color, timestamp, level, message, RESET);

            System.out.println(logMessage);

            if (logFilePath != null) {
                try (FileWriter fileWriter = new FileWriter(logFilePath, true);
                     PrintWriter printWriter = new PrintWriter(fileWriter)) {
                    printWriter.println(logMessage);
                } catch (IOException e) {
                    System.err.println("Logger konnte nicht schreiben: " + e.getMessage());
                }
            }
        }
    }

    private String getColor(LogLevel level) {
        switch (level) {
            case INFO: return INFO_COLOR;
            case DEBUG: return DEBUG_COLOR;
            case WARN: return WARN_COLOR;
            case ERROR: return ERROR_COLOR;
            default: return RESET;
        }
    }

    public void info(String message) {
        log(LogLevel.INFO, message);
    }

    public void debug(String message) {
        log(LogLevel.DEBUG, message);
    }

    public void warn(String message) {
        log(LogLevel.WARN, message);
    }

    public void error(String message) {
        log(LogLevel.ERROR, message);
    }

}