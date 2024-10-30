package org.example.Hibernate;

import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.util.Date;

public class HibernateConnection {

    public static void main(String[] args) {
        SessionFactory sessionFactory = new Configuration().configure().buildSessionFactory();

        Session session = sessionFactory.openSession();

        //Create
        Transaction transaction = session.beginTransaction();
        person person = new person("Max", "max@example.com", new Date());
        session.save(person);
        transaction.commit();

        //Read
        person person2 = session.get(person.class, 1L); // Benutzer mit ID 1 lesen
        System.out.println(person2.getName());

        //Update
        Transaction updateTransaciton = session.beginTransaction();
        person.setName("Max Mustermann");
        session.update(person);
        transaction.commit();

        //Delete
        Transaction deleteTransaction = session.beginTransaction();
        session.delete(person);
        transaction.commit();

        session.close();
        sessionFactory.close();

    }





}
