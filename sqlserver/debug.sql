USE CdcDemo;
GO

-- ── INSPECT ───────────────────────────────────────────────────────────────────
-- Run these anytime to see current state.

SELECT * FROM dbo.customers;
SELECT * FROM cdc.dbo_customers_CT;  -- raw CDC change table (op: 1=delete, 2=insert, 3=before-update, 4=after-update)

-- ── INSERT ────────────────────────────────────────────────────────────────────
-- op = "c" (create) in Debezium. before = null, after = new row.

INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city)
VALUES (4, N'Diana', N'Prince', N'diana@example.com', N'Themyscira');

INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city)
VALUES (5, N'Bruce', N'Wayne', N'bruce@example.com', N'Gotham');

-- ── UPDATE ────────────────────────────────────────────────────────────────────
-- op = "u" (update) in Debezium. before = old row, after = new row.
-- Only changed fields are highlighted in consumer.py output.

UPDATE dbo.customers SET city = 'Metropolis' WHERE customer_id = 1;
UPDATE dbo.customers SET city = 'Central City', email = 'alice.updated@example.com' WHERE customer_id = 1;

-- ── DELETE ────────────────────────────────────────────────────────────────────
-- op = "d" (delete) in Debezium. before = deleted row, after = null.

DELETE FROM dbo.customers WHERE customer_id = 5;

-- ── MULTI-ROW ─────────────────────────────────────────────────────────────────
-- Each row produces its own CDC event — you'll see one event per row, not one batch.

INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city)
VALUES
    (6, N'Clark', N'Kent',  N'clark@example.com', N'Smallville'),
    (7, N'Barry', N'Allen', N'barry@example.com', N'Central City');

DELETE FROM dbo.customers WHERE customer_id IN (6, 7);

-- ── RESET (back to seeded state) ──────────────────────────────────────────────
-- Wipe everything and re-seed the original 3 rows for a clean replay.

DELETE FROM dbo.customers;

INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city)
VALUES
    (1, N'Alice',   N'Smith', N'alice@example.com',   N'New York'),
    (2, N'Bob',     N'Jones', N'bob@example.com',     N'Los Angeles'),
    (3, N'Charlie', N'Brown', N'charlie@example.com', N'Chicago');
GO
