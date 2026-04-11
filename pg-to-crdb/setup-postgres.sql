-- PostgreSQL partitioned table demo for PG -> CockroachDB replication
-- Demonstrates the common scenario: many partitioned tables merged into a single target table

-- Create the parent orders table, partitioned by range on created_at
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL,
    order_number VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(200),
    amount NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    currency VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    region VARCHAR(50) NOT NULL DEFAULT 'us-east',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Generate 48 monthly partitions (Jan 2023 through Dec 2026)
-- This simulates the real-world scenario of hundreds of child partition tables
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
BEGIN
    FOR y IN 2023..2026 LOOP
        FOR m IN 1..12 LOOP
            start_date := make_date(y, m, 1);
            end_date := start_date + INTERVAL '1 month';
            partition_name := 'orders_' || to_char(start_date, 'YYYY_MM');
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %I PARTITION OF orders FOR VALUES FROM (%L) TO (%L)',
                partition_name, start_date, end_date
            );
        END LOOP;
    END LOOP;
END $$;

-- Create a non-partitioned customers table (for comparison)
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    email VARCHAR(200) UNIQUE NOT NULL,
    tier VARCHAR(20) DEFAULT 'standard',
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Insert sample customers
INSERT INTO customers (name, email, tier) VALUES
    ('Alice Johnson', 'alice@example.com', 'gold'),
    ('Bob Smith', 'bob@example.com', 'standard'),
    ('Carol Davis', 'carol@example.com', 'platinum'),
    ('Dan Evans', 'dan@example.com', 'standard'),
    ('Eve Foster', 'eve@example.com', 'gold')
ON CONFLICT DO NOTHING;

-- Insert orders spread across many partitions to demonstrate partition merging at scale
-- Generates ~200 orders distributed across the 48 monthly partitions
DO $$
DECLARE
    names TEXT[] := ARRAY['Alice Johnson','Bob Smith','Carol Davis','Dan Evans','Eve Foster'];
    emails TEXT[] := ARRAY['alice@example.com','bob@example.com','carol@example.com','dan@example.com','eve@example.com'];
    statuses TEXT[] := ARRAY['new','pending','confirmed','shipped','delivered'];
    regions TEXT[] := ARRAY['us-east','us-west','eu-west','eu-central','apac'];
    ord_date TIMESTAMPTZ;
    cust_idx INT;
BEGIN
    FOR y IN 2023..2026 LOOP
        FOR m IN 1..12 LOOP
            -- Insert ~4 orders per month across different partitions
            FOR d IN 1..4 LOOP
                cust_idx := ((y * 12 + m + d) % 5) + 1;
                ord_date := make_date(y, m, LEAST(d * 7, 28)) + INTERVAL '10 hours';
                INSERT INTO orders (order_number, customer_name, email, amount, status, region, created_at)
                VALUES (
                    'ORD-' || to_char(ord_date, 'YYYYMM') || '-' || LPAD(d::TEXT, 3, '0'),
                    names[cust_idx],
                    emails[cust_idx],
                    ROUND((random() * 500 + 10)::NUMERIC, 2),
                    statuses[(d % 5) + 1],
                    regions[(m % 5) + 1],
                    ord_date
                );
            END LOOP;
        END LOOP;
    END LOOP;
END $$;

-- Show partition layout summary
SELECT
    (SELECT count(*) FROM pg_inherits WHERE inhparent = 'orders'::regclass) AS total_partitions,
    count(*) AS total_orders
FROM orders;

-- Show distribution across partitions
SELECT tableoid::regclass AS partition_name, count(*) AS row_count
FROM orders
GROUP BY tableoid
ORDER BY partition_name;
