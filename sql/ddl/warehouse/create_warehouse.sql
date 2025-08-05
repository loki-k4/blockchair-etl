CREATE OR REPLACE WAREHOUSE BLOCKCHAIR_WH
COMMENT='Warehouse for Crypto. Used for ETL processes and reporting.'
WITH
    WAREHOUSE_SIZE = 'XSMALL',
    AUTO_SUSPEND = 300,
    AUTO_RESUME = TRUE;
-- This warehouse is designed to handle the ETL workloads for the BLOCKCHAIR database.
-- It is configured to automatically suspend after 5 minutes of inactivity and resume when needed.
-- The size is set to XSMALL to optimize costs while still providing sufficient resources for the ETL tasks.
-- Adjust the size and auto-suspend settings based on your workload requirements.   