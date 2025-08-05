CREATE OR REPLACE TABLE inputs_raw (
    BLOCK_ID INTEGER,
    TRANSACTION_HASH VARCHAR(64),
    INDEX INTEGER,
    TIME TIMESTAMP,
    VALUE INTEGER,
    VALUE_USD FLOAT,
    RECIPIENT VARCHAR(64),
    TYPE VARCHAR(32),
    SCRIPT_HEX VARCHAR(128),
    IS_FROM_COINBASE INTEGER,
    IS_SPENDABLE INTEGER,
    SPENDING_BLOCK_ID INTEGER,
    SPENDING_TRANSACTION_HASH VARCHAR(64),
    SPENDING_INDEX INTEGER,
    SPENDING_TIME TIMESTAMP,
    SPENDING_VALUE_USD FLOAT,
    SPENDING_SEQUENCE INTEGER,
    SPENDING_SIGNATURE_HEX VARCHAR(1024),
    SPENDING_WITNESS VARCHAR(8388608),
    LIFESPAN INTEGER,
    CDD FLOAT
);

-- This table is designed to store raw input data for blockchain transactions.
-- It includes various attributes such as block ID, transaction hash, input index, timestamps, values, and recipient addresses.
-- The table is optimized for ETL processes, allowing for efficient data retrieval and analysis.
-- Ensure that the necessary permissions and roles are set up for users who will access this table.
-- Adjust the data types and sizes based on your specific requirements and workload characteristics.
-- The table also includes fields for spending details, allowing for tracking of input usage across transactions.
-- The CDD (Coinbase Dependency Duration) field is included to analyze the dependency of inputs on coinbase transactions.
-- The LIFESPAN field can be used to track the duration of inputs from creation to spending.
-- The SPENDING_WITNESS field is designed to store witness data for SegWit transactions, allowing for analysis of witness inputs.
-- The SPENDING_SIGNATURE_HEX field is included to store the signature in hexadecimal format for verification purposes.
-- The IS_FROM_COINBASE field indicates whether the input is from a coinbase transaction, which is crucial for understanding the origin of the input.
-- The IS_SPENDABLE field indicates whether the input can be spent, which is important for transaction validation.
-- The SPENDING_SEQUENCE field is included to track the sequence number of the spending transaction, which can be useful for analyzing transaction order and dependencies.
-- The SPENDING_TRANSACTION_HASH and SPENDING_INDEX fields are included to link the input to its spending transaction, allowing for detailed analysis of input usage.
-- The RECIPIENT field is included to store the address that receives the input value, which is essential for tracking the flow of funds in the blockchain.
-- The TYPE field indicates the type of input, such as standard, witness, or coinbase, which can be useful for categorizing inputs and understanding their characteristics.
-- The SCRIPT_HEX field is included to store the script in hexadecimal format, which is important for analyzing the script structure and functionality.
-- The VALUE_USD field is included to store the value of the input in USD, allowing for financial analysis and reporting.
-- The TIME field is included to store the timestamp of the input, which is crucial for temporal analysis and understanding the timing of transactions.
-- The INDEX field is included to store the index of the input within the transaction, which is important for identifying the specific input in multi-input transactions.
-- The BLOCK_ID field is included to link the input to its corresponding block, allowing for efficient retrieval of block-level data.
-- The TRANSACTION_HASH field is included to link the input to its corresponding transaction, allowing for efficient retrieval of transaction-level data.
-- The table is designed to handle large volumes of input data efficiently, with appropriate indexing and partitioning strategies to optimize query performance.
-- Ensure that the necessary indexing strategies are applied to optimize query performance, especially for fields frequently used in filtering and joining operations.  