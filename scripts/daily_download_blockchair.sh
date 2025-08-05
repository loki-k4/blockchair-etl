#!/bin/bash

# Version: 1.0.7
# Purpose: Run download_blockchair_data.py in venv daily at 12:30 AM for Bitcoin data, generate DDL to sql/ddl/create_<table>.sql, skip existing DDLs with larger types, and retain last 2 days of data
# Usage: Called by cron, no manual execution required
# Example cron entry: 30 0 * * * /bin/bash /root/blockchair-etl/scripts/daily_download_blockchair.sh

SCRIPT_VERSION="1.0.7"
PROJECT_ROOT="$(realpath "$(dirname "$0")/..")"
LOG_DIR="${PROJECT_ROOT}/logs/downloader"
LOG_FILE="${LOG_DIR}/downloader_$(date +%Y%m%d).log"
DATA_DIR="${PROJECT_ROOT}/crypto-data/bitcoin"
SQL_DIR_DDL="${PROJECT_ROOT}/sql/ddl"
VENV_ACTIVATE="${PROJECT_ROOT}/.venv/bin/activate"
HOSTNAME=$(hostname)
YESTERDAY=$(date -d "yesterday" +%Y%m%d)

# Log message
log_message() {
    local level=$1
    local message=$2
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] [Host: $HOSTNAME] [Version: $SCRIPT_VERSION] [daily_download_blockchair] $message" >> "$LOG_FILE"
}

# Setup logging and SQL DDL directories
mkdir -p "$LOG_DIR" "$SQL_DIR_DDL"
if [[ $? -ne 0 ]]; then
    log_message "ERROR" "Failed to create directories: $LOG_DIR or $SQL_DIR_DDL"
    exit 1
fi

# Check if virtual environment exists
if [[ ! -f "$VENV_ACTIVATE" ]]; then
    log_message "ERROR" "Virtual environment not found at $VENV_ACTIVATE"
    exit 1
fi

log_message "INFO" "Starting daily download and DDL generation for Bitcoin data ($YESTERDAY)"

# Activate virtual environment
source "$VENV_ACTIVATE"

# Run download_blockchair_data.py
python3 "${PROJECT_ROOT}/utils/download_blockchair_data.py" bitcoin 1 blocks transactions outputs inputs \
    --config "${PROJECT_ROOT}/utils/config/download_config.json" \
    --log-dir "$LOG_DIR" \
    --log-level INFO \
    --skip-existing

if [[ $? -eq 0 ]]; then
    log_message "INFO" "Completed download of Bitcoin data for $YESTERDAY"
else
    log_message "ERROR" "Failed to download Bitcoin data for $YESTERDAY"
    deactivate
    exit 1
fi

# Generate DDL for each downloaded file
TABLES=("blocks" "transactions" "outputs" "inputs")
for table in "${TABLES[@]}"; do
    file="${DATA_DIR}/blockchair_bitcoin_${table}_${YESTERDAY}.tsv.gz"
    ddl_file="${SQL_DIR_DDL}/create_${table}.sql"
    if [[ -f "$file" ]]; then
        log_message "INFO" "Generating DDL for $file"
        python3 "${PROJECT_ROOT}/utils/generate_snowflake_ddl.py" \
            "$file" \
            "${table}_raw" \
            --sample-rows 100000 \
            --chunk-size 10000 \
            --config "${PROJECT_ROOT}/utils/config/ddl_config.json" \
            --log-dir "${PROJECT_ROOT}/logs/ddl_generator" \
            --log-level INFO \
            --output-ddl "$ddl_file" \
            --no-console-logs \
            --skip-existing
        exit_code=$?
        if [[ $exit_code -eq 0 ]]; then
            log_message "INFO" "Generated new DDL for ${table}_raw at $ddl_file"
        elif [[ $exit_code -eq 1 ]]; then
            log_message "INFO" "Kept existing DDL for ${table}_raw at $ddl_file due to larger schema"
        else
            log_message "WARNING" "Failed to generate DDL for $file (exit code: $exit_code)"
        fi
    else
        log_message "WARNING" "File not found: $file, skipping DDL generation"
    fi
done

deactivate

# Delete data files older than 2 days
log_message "INFO" "Deleting Bitcoin data older than 2 days from $DATA_DIR"
find "$DATA_DIR" -type f -name "*.tsv.gz" -mtime +2 -delete
if [[ $? -eq 0 ]]; then
    log_message "INFO" "Successfully deleted data files older than 2 days"
else
    log_message "WARNING" "Failed to delete some or all data files older than 2 days"
fi

log_message "INFO" "Completed daily download and DDL generation for Bitcoin data ($YESTERDAY)"