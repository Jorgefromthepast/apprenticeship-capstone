CREATE SCHEMA IF NOT EXISTS apprenticeship;
CREATE TABLE IF NOT EXISTS apprenticeship.user_purchase (
    invoice_number VARCHAR(10),
    stock_code VARCHAR(20),
    detail VARCHAR(1000),
    quantity INT,
    invoice_date TIMESTAMP,
    unit_price NUMERIC(8,3),
    customer_id INT,
    country VARCHAR(20)
    );