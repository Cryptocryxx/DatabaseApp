-- Erstellen der Tabellen mit 1:1, 1:n und n:m Beziehungen

-- Tabelle Person (1:1 Beziehung zu PersonDetail, 1:n zu Order)
CREATE TABLE Person (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        email VARCHAR(100) UNIQUE NOT NULL,
                        birth_date DATE NOT NULL
);

-- Tabelle PersonDetail (1:1 Beziehung zu Person)
CREATE TABLE PersonDetail (
                              id SERIAL PRIMARY KEY,
                              person_id INT UNIQUE, -- Unique Constraint für 1:1 Beziehung
                              address VARCHAR(255) NOT NULL,
                              phone_number VARCHAR(20),
                              FOREIGN KEY (person_id) REFERENCES Person (id) ON DELETE CASCADE
);

-- Tabelle Order (1:n Beziehung zu Person)
CREATE TABLE "Order" (
                         id SERIAL PRIMARY KEY,
                         order_date DATE NOT NULL,
                         total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount > 0),
                         person_id INT NOT NULL,
                         FOREIGN KEY (person_id) REFERENCES Person (id) ON DELETE CASCADE
);

-- Tabelle Product
CREATE TABLE Product (
                         id SERIAL PRIMARY KEY,
                         name VARCHAR(100) NOT NULL,
                         price DECIMAL(10, 2) NOT NULL CHECK (price > 0)
);

-- n:m Beziehung zwischen Order und Product (Bestellung enthält mehrere Produkte)
CREATE TABLE OrderProduct (
                              order_id INT,
                              product_id INT,
                              quantity INT NOT NULL CHECK (quantity > 0),
                              PRIMARY KEY (order_id, product_id),
                              FOREIGN KEY (order_id) REFERENCES "Order" (id) ON DELETE CASCADE,
                              FOREIGN KEY (product_id) REFERENCES Product (id) ON DELETE CASCADE
);