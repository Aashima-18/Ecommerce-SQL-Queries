create database olist_data;
use olist_data;

CREATE TABLE customers (
  customer_id VARCHAR(50) PRIMARY KEY,
  customer_unique_id VARCHAR(50),
  customer_zip_code_prefix INT,
  customer_city VARCHAR(100),
  customer_state VARCHAR(50)
);
CREATE TABLE orders (
  order_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50),
  order_status VARCHAR(50),
  order_purchase_timestamp DATETIME,
  order_approved_at DATETIME,
  order_delivered_carrier_date DATETIME,
  order_delivered_customer_date DATETIME,
  order_estimated_delivery_date DATETIME
);

CREATE TABLE order_items (
  order_id VARCHAR(50),
  order_item_id INT,
  product_id VARCHAR(50),
  seller_id VARCHAR(50),
  shipping_limit_date DATETIME,
  price DECIMAL(10,2),
  freight_value DECIMAL(10,2),
  PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE order_payments (
  order_id VARCHAR(50),
  payment_sequential INT,
  payment_type VARCHAR(50),
  payment_installments INT,
  payment_value DECIMAL(10,2),
  PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE order_reviews (
  review_id VARCHAR(50) PRIMARY KEY,
  order_id VARCHAR(50),
  review_score INT,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date DATETIME,
  review_answer_timestamp DATETIME
);

CREATE TABLE products (
  product_id VARCHAR(50) PRIMARY KEY,
  product_category_name VARCHAR(100),
  product_name_length INT,
  product_description_length INT,
  product_photos_qty INT,
  product_weight_g INT,
  product_length_cm INT,
  product_height_cm INT,
  product_width_cm INT
);

CREATE TABLE sellers (
  seller_id VARCHAR(50) PRIMARY KEY,
  seller_zip_code_prefix INT,
  seller_city VARCHAR(100),
  seller_state VARCHAR(10)
);

CREATE TABLE geolocation (
  geolocation_zip_code_prefix INT,
  geolocation_lat FLOAT,
  geolocation_lng FLOAT,
  geolocation_city VARCHAR(100),
  geolocation_state VARCHAR(10)
);

create table product_category_translation (
  product_category_name VARCHAR(100) PRIMARY KEY,
  product_category_name_english VARCHAR(100)
);

SHOW VARIABLES LIKE "secure_file_priv";

SET GLOBAL local_infile = 1;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
REPLACE INTO TABLE customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/product_category_name.csv'
IGNORE INTO TABLE product_category_translation
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
IGNORE INTO TABLE order_items
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
IGNORE INTO TABLE order_payments
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_reviews_dataset.csv'
IGNORE INTO TABLE order_reviews
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
IGNORE INTO TABLE orders
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
IGNORE INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_sellers_dataset.csv'
IGNORE INTO TABLE sellers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from customers;

SELECT 
  (SELECT COUNT(*) FROM orders) AS total_orders,
  (SELECT COUNT(DISTINCT customer_id) FROM customers) AS total_customers,
  (SELECT COUNT(DISTINCT seller_id) FROM sellers) AS total_sellers;
  
  SELECT * FROM olist_monthly_sales_enhanced
ORDER BY total_revenue DESC
LIMIT 5;

SELECT product_category_name, avg_order_value
FROM olist_category_revenue_enhanced
ORDER BY avg_order_value DESC
LIMIT 5;

  
  SELECT 
  DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS month,
  COUNT(*) AS order_count
FROM orders
GROUP BY month
ORDER BY month;

SELECT 
  product_id,
  COUNT(*) AS times_sold
FROM order_items
GROUP BY product_id
ORDER BY times_sold DESC
LIMIT 10;

SELECT 
  customer_city,
  COUNT(*) AS order_count
FROM customers
JOIN orders ON customers.customer_id = orders.customer_id
GROUP BY customer_city
ORDER BY order_count DESC
LIMIT 10;

SELECT 
  ROUND(AVG(DATEDIFF(order_delivered_customer_date, order_purchase_timestamp)), 2) AS avg_delivery_days
FROM orders
WHERE order_delivered_customer_date IS NOT NULL;

SELECT 
  oi.product_id,
  ROUND(AVG(review_score), 2) AS avg_review
FROM order_items oi
JOIN order_reviews r ON oi.order_id = r.order_id
GROUP BY oi.product_id
ORDER BY avg_review DESC
LIMIT 10;

SELECT 
  payment_type,
  COUNT(*) AS num_payments,
  ROUND(SUM(payment_value), 2) AS total_payment
FROM order_payments
GROUP BY payment_type
ORDER BY total_payment DESC;

CREATE OR REPLACE VIEW olist_order_summary AS
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS delivery_days,
    r.review_score,
    p.payment_type,
    p.payment_value,
    i.product_id,
    pr.product_category_name
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_reviews r ON o.order_id = r.order_id
LEFT JOIN order_payments p ON o.order_id = p.order_id
LEFT JOIN order_items i ON o.order_id = i.order_id
LEFT JOIN products pr ON i.product_id = pr.product_id;

SELECT customer_state, ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM olist_order_summary
WHERE delivery_days IS NOT NULL
GROUP BY customer_state;

SELECT product_category_name, ROUND(SUM(payment_value), 2) AS total_sales
FROM olist_order_summary
GROUP BY product_category_name
ORDER BY total_sales DESC
LIMIT 10;


CREATE OR REPLACE VIEW olist_monthly_sales AS
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM orders o
JOIN order_payments p ON o.order_id = p.order_id
GROUP BY month
ORDER BY month;

CREATE OR REPLACE VIEW olist_category_revenue AS
SELECT 
    pr.product_category_name,
    ROUND(SUM(p.payment_value), 2) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders
FROM orders o
JOIN order_items i ON o.order_id = i.order_id
JOIN products pr ON i.product_id = pr.product_id
JOIN order_payments p ON o.order_id = p.order_id
GROUP BY pr.product_category_name
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW olist_customer_ltv AS
SELECT 
    o.customer_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(p.payment_value), 2) AS total_spent,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_payments p ON o.order_id = p.order_id
GROUP BY o.customer_id, c.customer_city, c.customer_state
ORDER BY total_spent DESC;

SELECT * FROM olist_monthly_sales;

CREATE OR REPLACE VIEW olist_monthly_sales_enhanced AS
SELECT 
    DATE_FORMAT(order_purchase_timestamp, '%Y-%m') AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(SUM(p.payment_value), 2) AS total_revenue,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value,
    MAX(p.payment_value) AS max_order_value,
    MIN(p.payment_value) AS min_order_value
FROM orders o
JOIN order_payments p ON o.order_id = p.order_id
GROUP BY month
ORDER BY month;

CREATE OR REPLACE VIEW olist_category_revenue_enhanced AS
SELECT 
    pr.product_category_name,
    ROUND(SUM(p.payment_value), 2) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value,
    MAX(p.payment_value) AS max_payment,
    MIN(p.payment_value) AS min_payment
FROM orders o
JOIN order_items i ON o.order_id = i.order_id
JOIN products pr ON i.product_id = pr.product_id
JOIN order_payments p ON o.order_id = p.order_id
GROUP BY pr.product_category_name
ORDER BY total_revenue DESC;

-- For joining orders with customers
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- For joining orders with order_payments
CREATE INDEX idx_order_payments_order_id ON order_payments(order_id);

-- For joining orders with order_items
CREATE INDEX idx_order_items_order_id ON order_items(order_id);

-- For joining order_items with products
CREATE INDEX idx_products_product_id ON products(product_id);

-- For joining orders with order_reviews
CREATE INDEX idx_order_reviews_order_id ON order_reviews(order_id);

-- For filtering by purchase timestamp (e.g., monthly aggregations)
CREATE INDEX idx_orders_purchase_timestamp ON orders(order_purchase_timestamp);

-- For filtering by product category name
CREATE INDEX idx_products_category_name ON products(product_category_name);

-- For filtering customers by city or state
CREATE INDEX idx_customers_city ON customers(customer_city);
CREATE INDEX idx_customers_state ON customers(customer_state);

SHOW INDEXES FROM orders;
SHOW INDEXES FROM customers;
