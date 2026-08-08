-- =====================================================================
-- E-Commerce OLTP Schema (MySQL / AWS RDS)
-- This represents the "source of truth" operational database that the
-- Databricks pipeline extracts from.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

-- ---------------------------------------------------------------------
-- customers: mutable dimension source -> feeds SCD Type 2 in gold layer
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customers (
    customer_id     BIGINT PRIMARY KEY AUTO_INCREMENT,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    address         VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    postal_code     VARCHAR(20),
    country         VARCHAR(100),
    loyalty_tier    VARCHAR(20)     DEFAULT 'BRONZE',   -- BRONZE/SILVER/GOLD/PLATINUM, changes over time
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- products
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products (
    product_id      BIGINT PRIMARY KEY AUTO_INCREMENT,
    sku             VARCHAR(50) UNIQUE,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    unit_price      DECIMAL(10,2),
    created_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------
-- orders
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGINT PRIMARY KEY AUTO_INCREMENT,
    customer_id     BIGINT NOT NULL,
    order_status    VARCHAR(20)     DEFAULT 'PLACED',   -- PLACED/SHIPPED/DELIVERED/CANCELLED/RETURNED
    order_date      DATETIME        NOT NULL,
    shipping_cost   DECIMAL(10,2)   DEFAULT 0.00,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- ---------------------------------------------------------------------
-- order_items
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id   BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id        BIGINT NOT NULL,
    product_id      BIGINT NOT NULL,
    quantity        INT             NOT NULL,
    unit_price      DECIMAL(10,2)   NOT NULL,           -- price AT time of purchase (can differ from current product price)
    discount_pct    DECIMAL(5,2)    DEFAULT 0.00,
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- ---------------------------------------------------------------------
-- inventory
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_id      BIGINT NOT NULL,
    warehouse_code  VARCHAR(20),
    quantity_on_hand INT            DEFAULT 0,
    reorder_level   INT             DEFAULT 10,
    updated_at      TIMESTAMP       DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Helpful indexes for incremental extraction (watermark-based CDC)
CREATE INDEX idx_customers_updated_at ON customers(updated_at);
CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_inventory_updated_at ON inventory(updated_at);
