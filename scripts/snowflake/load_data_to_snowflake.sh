#!/bin/bash

# Script Name: load_files.sh
# Description: Loads Bitcoin data from TSV files into Snowflake tables and cleans up stage files.
# Version: 1.0.3
# Date: August 15, 2025
# Usage: ./load_files.sh
# Dependencies: Bash 4.0+, snowsql
# Notes:
#   - Logs timestamps in EDT (America/New_York, UTC-4) with format YYYY-MM-DDTHH:MM:SS-04:00 in JSON format.
#   - Loads data from $DATA_DIR/{blocks,inputs,outputs,transactions}/ into Snowflake tables.
#   - Sources environment variables from config/.env for Snowflake connection and data settings.
#   - Logs to $LOG_DIR/snowflake/load_files_YYYYMMDD.log.
#   - Removes stage files older than $RETENTION_DAYS days.

# Enable strict mode for error handling
set -euo pipefail

# Exit codes
EXIT_SUCCESS=0
EXIT_CONFIG_ERROR=1
EXIT_EXECUTION_ERROR=2

# Constants
SCRIPT_VERSION="1.0.3"
SCRIPT_NAME=$(basename "$0")
PROJECT_DIR="/root/blockchair-etl"
ENV_FILE="${PROJECT_DIR}/config/.env"
TABLES=("blocks" "transactions" "inputs" "outputs")
STAGE_NAME="tsv_file_stage"
FILE_FORMAT="tsv_file_format"
SESSION_ID=$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid 2>/dev/null)
SCRIPT_USER=$(whoami)
HOSTNAME=$(hostname)

# Log message in JSON format
log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(TZ=America/New_York date '+%Y-%m-%dT%H:%M:%S-04:00')
    local log_entry
    log_entry=$(printf '{"timestamp":"%s","script":"%s","version":"%s","session_id":"%s","user":"%s","host":"%s","level":"%s","message":"%s"}' \
        "$timestamp" "$SCRIPT_NAME" "$SCRIPT_VERSION" "$SESSION_ID" "$SCRIPT_USER" "$HOSTNAME" "$level" "$message")
    echo "$log_entry" | tee -a "$LOG_FILE"
}

# Setup logging directory
setup_logging() {
    if [ -z "${LOG_DIR:-}" ]; then
        echo "ERROR: LOG_DIR environment variable not set" >&2
        exit $EXIT_CONFIG_ERROR
    fi
    if ! mkdir -p "$LOG_DIR/snowflake"; then
        echo "ERROR: Failed to create log directory: $LOG_DIR/snowflake" >&2
        exit $EXIT_CONFIG_ERROR
    fi
    LOG_FILE="${LOG_DIR}/snowflake/${SCRIPT_NAME}_$(TZ=America/New_York date +%Y%m%d).log"
}

# Source .env file
source_env() {
    if [ ! -f "$ENV_FILE" ]; then
        log_message "ERROR" ".env file not found at $ENV_FILE"
        exit $EXIT_CONFIG_ERROR
    fi
    set -a
    source "$ENV_FILE"
    set +a
    if [ -z "${LOG_DIR:-}" ]; then
        log_message "ERROR" "LOG_DIR not set in $ENV_FILE"
        exit $EXIT_CONFIG_ERROR
    fi
    log_message "INFO" "Sourced environment variables from $ENV_FILE"
}

# Validate environment variables
validate_env_vars() {
    local required_vars=("SNOWSQL_ACCOUNT" "SNOWSQL_USER" "SNOWSQL_PWD" "SNOWSQL_ROLE" "SNOWSQL_DATABASE" "SNOWSQL_WAREHOUSE" "SNOWSQL_SCHEMA" "DATA_DIR" "LOG_DIR" "RETENTION_DAYS")
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            log_message "ERROR" "Environment variable $var is not set"
            exit $EXIT_CONFIG_ERROR
        fi
    done
    CLEANUP_DATE=$(TZ=America/New_York date -d "-${RETENTION_DAYS} days" +%Y%m%d)
}

# Validate SnowSQL installation
validate_snowsql() {
    if ! command -v snowsql >/dev/null; then
        log_message "ERROR" "SnowSQL not found in PATH. Run install_snowsql.sh first."
        exit $EXIT_CONFIG_ERROR
    fi
}

# Main logic
main() {
    setup_logging
    source_env
    log_message "INFO" "Starting data load to Snowflake. Session ID: $SESSION_ID"

    # Validate prerequisites
    validate_env_vars
    validate_snowsql

    # Load files to stage
    log_message "INFO" "Loading files to Snowflake stage $STAGE_NAME"
    for table in "${TABLES[@]}"; do
        local file_path="${DATA_DIR}/${table}/blockchair_bitcoin_${table}_*.tsv.gz"
        if ls $file_path >/dev/null 2>&1; then
            log_message "INFO" "Uploading files for $table to $STAGE_NAME"
            if ! snowsql -a "$SNOWSQL_ACCOUNT" -u "$SNOWSQL_USER" -r "$SNOWSQL_ROLE" -w "$SNOWSQL_WAREHOUSE" -d "$SNOWSQL_DATABASE" -s "$SNOWSQL_SCHEMA" \
                --variable password="$SNOWSQL_PWD" \
                -q "PUT file://${DATA_DIR}/${table}/blockchair_bitcoin_${table}_*.tsv.gz @$STAGE_NAME AUTO_COMPRESS=TRUE"; then
                log_message "ERROR" "Failed to upload files for $table to $STAGE_NAME"
                exit $EXIT_EXECUTION_ERROR
            fi
        else
            log_message "WARNING" "No files found for $table at $file_path"
        fi
    done

    # Copy stage files to tables
    for table in "${TABLES[@]}"; do
        local table_name="${table^^}_RAW"  # Convert to uppercase (e.g., blocks -> BLOCKS_RAW)
        log_message "INFO" "Copying files for $table_name from $STAGE_NAME"
        if ! snowsql -a "$SNOWSQL_ACCOUNT" -u "$SNOWSQL_USER" -r "$SNOWSQL_ROLE" -w "$SNOWSQL_WAREHOUSE" -d "$SNOWSQL_DATABASE" -s "$SNOWSQL_SCHEMA" \
            --variable password="$SNOWSQL_PWD" \
            -q "COPY INTO $table_name FROM @$STAGE_NAME FILE_FORMAT = (FORMAT_NAME = $FILE_FORMAT) PATTERN='.*${table}.*' ON_ERROR = 'skip_file'"; then
            log_message "ERROR" "Failed to copy files into $table_name"
            exit $EXIT_EXECUTION_ERROR
        fi
    done

    # Remove old files from stage
    log_message "INFO" "Removing files older than $RETENTION_DAYS days from $STAGE_NAME (pattern: .*$CLEANUP_DATE.*)"
    if ! snowsql -a "$SNOWSQL_ACCOUNT" -u "$SNOWSQL_USER" -r "$SNOWSQL_ROLE" -w "$SNOWSQL_WAREHOUSE" -d "$SNOWSQL_DATABASE" -s "$SNOWSQL_SCHEMA" \
        --variable password="$SNOWSQL_PWD" \
        -q "REMOVE @$STAGE_NAME PATTERN='.*$CLEANUP_DATE.*'"; then
        log_message "WARNING" "Failed to remove some or all files from $STAGE_NAME"
    else
        log_message "INFO" "Successfully removed files from $STAGE_NAME"
    fi

    log_message "INFO" "Completed data load to Snowflake"
    exit $EXIT_SUCCESS
}

# Run main
main