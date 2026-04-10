-- DML operations across multiple partitions to demonstrate CDC replication
-- Each operation hits a different monthly partition based on created_at

-- UPDATE: modify orders in different partitions (2023, 2024, 2025, 2026)
UPDATE orders SET status = 'delivered', updated_at = now()
    WHERE order_number = 'ORD-202302-001';

UPDATE orders SET amount = 299.99, status = 'shipped', updated_at = now()
    WHERE order_number = 'ORD-202408-001';

UPDATE orders SET status = 'cancelled', updated_at = now()
    WHERE order_number = 'ORD-202511-001';

-- INSERT: new order into a recent partition
INSERT INTO orders (order_number, customer_name, email, amount, status, region, created_at)
VALUES ('ORD-202604-NEW', 'Eve Foster', 'eve@example.com', 175.00, 'new', 'us-east', '2026-04-08 10:00:00+00');

-- DELETE: remove an order from an older partition
DELETE FROM orders WHERE order_number = 'ORD-202307-001';

-- Customer operations
UPDATE customers SET tier = 'platinum' WHERE email = 'alice@example.com';
INSERT INTO customers (name, email, tier) VALUES ('Frank Garcia', 'frank@example.com', 'standard')
ON CONFLICT DO NOTHING;
