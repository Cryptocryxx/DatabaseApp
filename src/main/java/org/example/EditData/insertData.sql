INSERT INTO Person (name, email, birth_date)
VALUES ('Alice Cooper', 'alice.cooper@example.com', '1985-09-12');
-- Fügt eine neue Person in die Tabelle `Person` ein.

INSERT INTO PersonDetail (person_id, address, phone_number)
VALUES (3, '789 Pine St, Villagetown', '555-666-7777');
-- Fügt ein neues PersonDetail hinzu, das sich auf die Person mit der ID 3 bezieht.

INSERT INTO "Order" (order_date, total_amount, person_id)
VALUES ('2024-03-01', 750.00, 3);
-- Fügt eine neue Bestellung für die Person mit ID 3 hinzu.

INSERT INTO Product (name, price)
VALUES ('Tablet', 600.25);
-- Fügt ein neues Produkt in die Tabelle `Product` ein.

INSERT INTO OrderProduct (order_id, product_id, quantity)
VALUES (3, 3, 5);
-- Fügt eine neue Beziehung zwischen der Bestellung 3 und dem Produkt 3 mit einer Menge von 5 hinzu.