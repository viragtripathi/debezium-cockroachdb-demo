-- Enable rangefeed (required for changefeeds)
SET CLUSTER SETTING kv.rangefeed.enabled = true;

CREATE DATABASE IF NOT EXISTS demodb;
USE demodb;

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_number STRING UNIQUE NOT NULL,
    customer_name STRING NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    status STRING NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT current_timestamp()
);

-- Reset to the exact seed rows on every run so reruns against a warm stack are
-- deterministic: leftover rows from a previous run stream through the changefeed as
-- deletes and the Oracle target converges to the same final state every time.
-- (DELETE, not TRUNCATE: changefeeds do not emit events for TRUNCATE.)
DELETE FROM orders;

INSERT INTO orders (order_number, customer_name, amount, status) VALUES
    ('ORD-1001', 'Alice Johnson', 129.99, 'confirmed'),
    ('ORD-1002', 'Bob Smith',     249.50, 'pending'),
    ('ORD-1003', 'Carol Davis',    34.99, 'shipped');
