#!/bin/bash
set -e

# Loading files to stage
snowsql -c bitcoin_raw --config .snowsql/config -q "PUT file://crypto-data/bitcoin/blockchair*.tsv.gz @tsv_file_stage AUTO_COMPRESS=TRUE"

# Copying stage files to tables
snowsql -c bitcoin_raw --config .snowsql/config -q "COPY INTO BLOCKS_RAW 
FROM @tsv_file_stage 
FILE_FORMAT = (FORMAT_NAME = tsv_file_format) 
PATTERN='.*blocks.*' 
ON_ERROR = 'skip_file';"

snowsql -c bitcoin_raw --config .snowsql/config -q "COPY INTO TRANSACTIONS_RAW 
FROM @tsv_file_stage 
FILE_FORMAT = (FORMAT_NAME = tsv_file_format) 
PATTERN='.*transactions.*' 
ON_ERROR = 'skip_file';"

snowsql -c bitcoin_raw --config .snowsql/config -q "COPY INTO INPUTS_RAW 
FROM @tsv_file_stage 
FILE_FORMAT = (FORMAT_NAME = tsv_file_format) 
PATTERN='.*inputs.*' 
ON_ERROR = 'skip_file';"

snowsql -c bitcoin_raw --config .snowsql/config -q "COPY INTO OUTPUTS_RAW 
FROM @tsv_file_stage 
FILE_FORMAT = (FORMAT_NAME = tsv_file_format) 
PATTERN='.*outputs.*' 
ON_ERROR = 'skip_file';"

# Remove files from stage after loading
snowsql -c bitcoin_raw --config .snowsql/config -q "REMOVE @tsv_file_stage PATTERN='.*$(date -d '-4 days' +%Y%m%d).*' "