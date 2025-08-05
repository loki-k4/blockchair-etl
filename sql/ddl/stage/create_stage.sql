CREATE OR REPLACE STAGE tsv_file_staged
FILE_FORMAT = tsv_file
COMMENT = 'Stage for TSV files used in blockchain ETL processes.';

-- This stage is designed to facilitate the loading of TSV (Tab-Separated Values) files into the BLOCKCHAIR database.
-- It uses the tsv_file file format, which specifies that fields are separated by tabs and that there is no optional enclosure for fields.
-- The stage allows for efficient data loading and processing in the ETL pipeline for blockchain data.
-- Ensure that the necessary permissions and roles are set up for users who will access this stage.
-- The stage is intended to be used in conjunction with the BLOCKCHAIR database and schemas to facilitate the ingestion of raw blockchain data.
-- The tsv_file_staged stage is particularly useful for handling large datasets where tab separation is preferred for readability and processing efficiency.