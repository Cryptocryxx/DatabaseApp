package org.example.Hibernate;

import org.example.Hibernate.Table;

import java.util.List;


public class Database {

    private List<Table> tables;
    private String name;

    // Standardkonstruktor
    public Database() {}

    // Parameterisierter Konstruktor
    public Database(String name) {
        this.name = name;
    }

    // Getter und Setter
    public void addTable(String tableName) {
        tables.add(new Table(tableName));
    }
}
