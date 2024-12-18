UPDATE Person
SET email = 'john.doe_updated@example.com'
WHERE name = 'John Doe';
-- Aktualisiert die E-Mail-Adresse von `John Doe`.

UPDATE PersonDetail
SET phone_number = '999-888-7777'
WHERE address = '123 Main St, Cityville';
-- Ändert die Telefonnummer der Person mit der angegebenen Adresse.

UPDATE "Order"
SET total_amount = 1450.50
WHERE id = 1;
-- Aktualisiert den Gesamtbetrag der Bestellung mit der ID 1.

UPDATE Product
SET price = 850.75
WHERE name = 'Smartphone';
-- Aktualisiert den Preis des Produkts `Smartphone`.

UPDATE OrderProduct
SET quantity = 3
WHERE order_id = 1 AND product_id = 3;
-- Ändert die Menge des Produkts mit der ID 3 in Bestellung 1 auf 3.

