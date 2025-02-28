package hdm.DataBack.Logger;

public class TestLogger {
    public static void main(String[] args) {
        Logger log = new Logger();

        log.info("Hello World");
        log.debug("Hello World");
        log.warn("Hello World");
        log.error("Hello World");
    }
}
