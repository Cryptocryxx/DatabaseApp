DELETE FROM Person
WHERE name = 'Jane Smith';
-- Löscht die Person mit dem Namen `Jane Smith` aus der Tabelle `Person`.

DELETE FROM PersonDetail
WHERE phone_number = '987-654-3210';
-- Löscht das Detail der Person, die diese Telefonnummer besitzt.

DELETE FROM "Order"
WHERE id = 2;
-- Löscht die Bestellung mit der ID 2.

DELETE FROM Product
WHERE name = 'Headphones';
-- Löscht das Produkt `Headphones`.

DELETE FROM OrderProduct
WHERE order_id = 2 AND product_id = 1;
-- Löscht die Zuordnung zwischen Bestellung 2 und Produkt 1.
