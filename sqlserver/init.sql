-- Runs once via the sqlserver-init helper container.
-- Creates CdcDemo, enables CDC at DB and table level, seeds 3 rows.

-- ── 1. Create the database ────────────────────────────────────────────────────
USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'CdcDemo')
BEGIN
    CREATE DATABASE CdcDemo;
END
GO

-- ── 2. Enable CDC at the database level ───────────────────────────────────────
-- Creates the cdc schema, system tables, and stored procedures.
USE CdcDemo;
GO

IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'CdcDemo') = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- ── 3. Create the customers table ─────────────────────────────────────────────
IF NOT EXISTS (
    SELECT 1 FROM sys.tables
    WHERE name = 'customers' AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE dbo.customers (
        customer_id   INT           NOT NULL PRIMARY KEY,
        first_name    NVARCHAR(100) NOT NULL,
        last_name     NVARCHAR(100) NOT NULL,
        email         NVARCHAR(255) NOT NULL,
        city          NVARCHAR(100) NULL,
        created_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- ── 4. Enable CDC on the customers table ──────────────────────────────────────
-- @role_name = NULL means no extra role is required (fine for local learning).
-- @supports_net_changes = 1 allows querying the net change per row per interval.
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('dbo.customers')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema        = N'dbo',
        @source_name          = N'customers',
        @role_name            = NULL,
        @supports_net_changes = 1;
END
GO

-- ── 5. Seed rows so the consumer shows events immediately on first run ─────────
INSERT INTO dbo.customers (customer_id, first_name, last_name, email, city)
VALUES
    (1, N'Alice',   N'Smith', N'alice@example.com',   N'New York'),
    (2, N'Bob',     N'Jones', N'bob@example.com',     N'Los Angeles'),
    (3, N'Charlie', N'Brown', N'charlie@example.com', N'Chicago');
GO

PRINT 'CdcDemo initialized: CDC enabled on dbo.customers, 3 rows seeded.';
GO
