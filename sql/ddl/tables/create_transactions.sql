CREATE OR REPLACE TABLE transactions_raw (
    BLOCK_ID INTEGER,
    HASH VARCHAR(64),
    TIME TIMESTAMP,
    SIZE INTEGER,
    WEIGHT INTEGER,
    VERSION INTEGER,
    LOCK_TIME INTEGER,
    IS_COINBASE INTEGER,
    HAS_WITNESS INTEGER,
    INPUT_COUNT INTEGER,
    OUTPUT_COUNT INTEGER,
    INPUT_TOTAL INTEGER,
    INPUT_TOTAL_USD FLOAT,
    OUTPUT_TOTAL INTEGER,
    OUTPUT_TOTAL_USD FLOAT,
    FEE INTEGER,
    FEE_USD FLOAT,
    FEE_PER_KB FLOAT,
    FEE_PER_KB_USD FLOAT,
    FEE_PER_KWU FLOAT,
    FEE_PER_KWU_USD FLOAT,
    CDD_TOTAL FLOAT
);

-- This table is designed to store raw transaction data for blockchain analytics.
-- It includes various attributes such as block ID, transaction hash, timestamps, sizes, and transaction details.
-- The table is optimized for ETL processes, allowing for efficient data retrieval and analysis.
-- Ensure that the necessary permissions and roles are set up for users who will access this table.
-- Adjust the data types and sizes based on your specific requirements and workload characteristics.
-- The IS_COINBASE field indicates whether the transaction is a coinbase transaction, which is crucial for understanding the origin of the transaction.
-- The HAS_WITNESS field indicates whether the transaction includes witness data, which is important for analyzing SegWit transactions.
-- The INPUT_COUNT and OUTPUT_COUNT fields are included to track the number of inputs and outputs in the transaction, which is essential for understanding transaction complexity.
-- The INPUT_TOTAL and OUTPUT_TOTAL fields are included to store the total input and output values in satoshis, allowing for financial analysis.
-- The INPUT_TOTAL_USD and OUTPUT_TOTAL_USD fields are included to store the total input and output values in USD, allowing for financial analysis and reporting.
-- The FEE field is included to store the transaction fee in satoshis, which is important for transaction validation and analysis.
-- The FEE_USD field is included to store the transaction fee in USD, allowing for financial analysis and reporting.
-- The FEE_PER_KB and FEE_PER_KB_USD fields are included to store the fee per kilobyte in satoshis and USD, respectively, which is useful for analyzing transaction costs relative to size.     