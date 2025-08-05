CREATE OR REPLACE TABLE blocks_raw (
    ID INTEGER,
    HASH VARCHAR(64),
    TIME TIMESTAMP,
    MEDIAN_TIME TIMESTAMP,
    SIZE INTEGER,
    STRIPPED_SIZE INTEGER,
    WEIGHT INTEGER,
    VERSION INTEGER,
    VERSION_HEX VARCHAR(16),
    VERSION_BITS VARCHAR(32),
    MERKLE_ROOT VARCHAR(64),
    NONCE INTEGER,
    BITS INTEGER,
    DIFFICULTY INTEGER,
    CHAINWORK VARCHAR(64),
    COINBASE_DATA_HEX VARCHAR(256),
    TRANSACTION_COUNT INTEGER,
    WITNESS_COUNT INTEGER,
    INPUT_COUNT INTEGER,
    OUTPUT_COUNT INTEGER,
    INPUT_TOTAL INTEGER,
    INPUT_TOTAL_USD FLOAT,
    OUTPUT_TOTAL INTEGER,
    OUTPUT_TOTAL_USD FLOAT,
    FEE_TOTAL INTEGER,
    FEE_TOTAL_USD FLOAT,
    FEE_PER_KB FLOAT,
    FEE_PER_KB_USD FLOAT,
    FEE_PER_KWU FLOAT,
    FEE_PER_KWU_USD FLOAT,
    CDD_TOTAL FLOAT,
    GENERATION INTEGER,
    GENERATION_USD FLOAT,
    REWARD INTEGER,
    REWARD_USD FLOAT,
    GUESSED_MINER VARCHAR(16)
);

-- This table is designed to store raw block data for blockchain analytics.
-- It includes various attributes such as block ID, hash, timestamps, sizes, and transaction details.
-- The table is optimized for ETL processes, allowing for efficient data retrieval and analysis.
-- Ensure that the necessary permissions and roles are set up for users who will access this table.
-- Adjust the data types and sizes based on your specific requirements and workload characteristics.
-- The table structure can be extended with additional fields as needed for future requirements.
-- Consider partitioning strategies if the dataset grows significantly to maintain performance.
-- The ID field is the primary key for the table, ensuring uniqueness for each block.
-- The HASH field is used to uniquely identify the block in the blockchain.
-- The TIME field stores the timestamp of when the block was mined, which is crucial for temporal analysis.
-- The MEDIAN_TIME field is included to store the median time of the block, which can be useful for understanding block timing and delays.
-- The SIZE and STRIPPED_SIZE fields store the size of the block in bytes, which is important for analyzing block size and capacity.
-- The WEIGHT field is included to store the weight of the block, which is relevant for SegWit transactions.
-- The VERSION and VERSION_HEX fields store the version of the block in both integer and hexadecimal formats, respectively, which can be useful for analyzing block features and compatibility.
-- The VERSION_BITS field is included to store the version bits of the block, which can be useful for understanding soft forks and consensus rules.
-- The MERKLE_ROOT field stores the root hash of the Merkle tree for the transactions in the block, which is essential for verifying transaction integrity.
-- The NONCE field is included to store the nonce used in the proof-of-work algorithm, which is crucial for mining and block validation.
-- The BITS field stores the target difficulty for the block, which is important for understanding mining difficulty and block generation.
-- The DIFFICULTY field is included to store the actual difficulty of the block, which can be useful for analyzing mining trends and network health.
-- The CHAINWORK field is included to store the cumulative work done on the blockchain up to this block, which is important for understanding the security and stability of the blockchain.
-- The COINBASE_DATA_HEX field is included to store the coinbase transaction data in hexadecimal format, which can be useful for analyzing the coinbase transaction and its properties.
-- The TRANSACTION_COUNT field stores the number of transactions included in the block, which is essential for understanding block activity and transaction volume.
-- The WITNESS_COUNT field is included to store the number of witness transactions in the block, which is relevant for SegWit analysis.
-- The INPUT_COUNT and OUTPUT_COUNT fields are included to track the number of inputs and outputs in the block, which is essential for understanding transaction complexity.
-- The INPUT_TOTAL and OUTPUT_TOTAL fields store the total input and output values in satoshis, allowing for financial analysis.
-- The INPUT_TOTAL_USD and OUTPUT_TOTAL_USD fields are included to store the total input and output values in USD, allowing for financial analysis and reporting.
-- The FEE_TOTAL field is included to store the total transaction fees in satoshis, which is important for transaction validation and analysis.
-- The FEE_TOTAL_USD field is included to store the total transaction fees in USD, allowing for financial analysis and reporting.
-- The FEE_PER_KB and FEE_PER_KB_USD fields are included to store the fee per kilobyte in satoshis and USD, respectively,
-- which is useful for analyzing transaction costs relative to size.
-- The FEE_PER_KWU and FEE_PER_KWU_USD fields are included to store the fee per kiloweight unit in satoshis and USD, respectively,
-- which is useful for analyzing transaction costs relative to weight.
-- The CDD_TOTAL field is included to store the total Coinbase Dependency Duration, which can be useful for analyzing the dependency of blocks on coinbase transactions.    
