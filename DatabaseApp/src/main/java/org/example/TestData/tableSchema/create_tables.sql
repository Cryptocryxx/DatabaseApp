CREATE TABLE person (
id INT,
birth_date date,
name VARCHAR(255),
email VARCHAR(255)
);

CREATE TABLE persondetail (
id INT,
person_id INT,
address VARCHAR(255),
phone_number VARCHAR(255)
);

CREATE TABLE Order (
id INT,
order_date date,
total_amount numeric,
person_id INT
);

CREATE TABLE orderproduct (
order_id INT,
product_id INT,
quantity INT
);

CREATE TABLE product (
id INT,
price numeric,
name VARCHAR(255)
);

