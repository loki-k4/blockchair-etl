CREATE OR REPLACE SCHEMA BLOCKCHAIR.BITCOIN_RAW
COMMENT='Schema for Bitcoin Raw Data. Contains raw blockchain data for Bitcoin transactions, blocks, and addresses.';

-- This schema is designed to store raw data related to Bitcoin transactions, blocks, and addresses.
-- It includes tables for transactions, outputs, and other relevant entities.
-- The schema is optimized for ETL processes, allowing for efficient data retrieval and analysis.
-- Ensure that the necessary permissions and roles are set up for users who will access this schema.
-- Adjust the schema settings based on your specific requirements and workload characteristics.
-- The schema is intended to be used in conjunction with the BLOCKCHAIR database, which contains the overall structure for blockchain analytics.
-- The BITCOIN_RAW schema is specifically focused on raw data collection, allowing for detailed analysis and reporting on Bitcoin transactions and blocks.
-- Consider partitioning strategies if the dataset grows significantly to maintain performance.
-- The schema can be extended with additional tables as needed for future requirements.
-- Ensure that the naming conventions for tables and fields are consistent with the overall BLOCKCHAIR database structure.
-- The BITCOIN_RAW schema is part of the larger BLOCKCHAIR ecosystem, which includes other cryptocurrencies and related data.
-- The schema is designed to be flexible and adaptable to future changes in the Bitcoin protocol or data requirements.
-- The BITCOIN_RAW schema is intended to provide a foundation for building more complex analytics and reporting solutions on top of the raw Bitcoin data.
-- The schema can be used in conjunction with other schemas in the BLOCKCHAIR database to create a comprehensive view of blockchain data across multiple cryptocurrencies.
-- The BITCOIN_RAW schema is a key component of the BLOCKCHAIR data architecture, enabling detailed analysis of Bitcoin transactions and blocks.
-- The schema is designed to support high-volume data ingestion and processing, making it suitable for large-scale blockchain analytics.
-- The BITCOIN_RAW schema is intended to be used by data analysts, developers, and researchers who require access to raw Bitcoin data for analysis and reporting.
-- The schema is designed to be easily extensible, allowing for the addition of new tables and fields as needed to accommodate future data requirements.
-- The BITCOIN_RAW schema is part of the BLOCKCHAIR data warehouse, which provides a centralized repository for blockchain data across multiple cryptocurrencies.
-- The schema is designed to support a wide range of analytics use cases, including transaction analysis, block analysis, and address analysis.
-- The BITCOIN_RAW schema is intended to provide a robust and flexible foundation for building blockchain analytics solutions.
-- The schema is designed to be compatible with various data processing frameworks and tools, enabling seamless integration with existing analytics workflows.