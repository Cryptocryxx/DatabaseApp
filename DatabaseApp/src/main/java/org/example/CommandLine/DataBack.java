package org.example.CommandLine;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "Calculator", description = "Performs basic arithmetic operations on two numbers.")
public class DataBack implements Runnable {

    @Parameters(index = "0", description = "The first number.")
    private double a;

    @Parameters(index = "1", description = "The second number.")
    private double b;

    @Option(names = {"-op", "--operation"}, description = "The operation to perform: +, -, *, or /", required = true)
    private String operation;

    @Override
    public void run() {
        double result;
        switch (operation) {
            case "+":
                result = a + b;
                System.out.printf("Result: %.2f + %.2f = %.2f%n", a, b, result);
                break;
            case "-":
                result = a - b;
                System.out.printf("Result: %.2f - %.2f = %.2f%n", a, b, result);
                break;
            case "*":
                result = a * b;
                System.out.printf("Result: %.2f * %.2f = %.2f%n", a, b, result);
                break;
            case "/":
                if (b != 0) {
                    result = a / b;
                    System.out.printf("Result: %.2f / %.2f = %.2f%n", a, b, result);
                } else {
                    System.out.println("Error: Division by zero is not allowed.");
                }
                break;
            default:
                System.out.println("Invalid operation. Please use +, -, *, or /.");
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DataBack()).execute(args);
        System.exit(exitCode);
    }
}
