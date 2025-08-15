#!/bin/bash

# Script Name: pipeline.sh
# Description: Orchestrates ETL pipeline tasks for the blockchair-etl project, including setting up environments,
#              downloading data, generating Snowflake DDLs, loading data, and cleaning old files.
# Version: 1.1.1
# Author: [Your Company/Author Name]
# Date: August 14, 2025
# Usage: ./pipeline.sh <command> [options]
# Dependencies: Bash 4.0+, Python 3, SnowSQL CLI, uuidgen (optional)
# Environment: Requires config/.env file with optional variables (e.g., DATA_DIR, LOG_DIR, RETENTION_DAYS)

# Exit on error, treat unset variables as errors
set -eu

# Exit codes
EXIT_SUCCESS=0
EXIT_INVALID_ARGS=1
EXIT_CONFIG_ERROR=2
EXIT_EXECUTION_ERROR=5

# Default configuration
VERSION="1.1.1"
SCRIPT_NAME=$(basename "$0")
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
LOG_FILE="${LOG_DIR}/blockchair_etl_$(date +%Y%m%d).log"
SESSION_ID=$(uuidgen || cat /proc/sys/kernel/random/uuid)
USER=$(whoami)
RETENTION_DAYS=3  # Default value to prevent unbound variable error

# Logging setup with structured JSON output
setup_logging() {
    mkdir -p "${LOG_DIR}"
    # Rotate logs (keep last specified days)
    find "${LOG_DIR}" -type f -name "blockchair_etl_*.log" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true
    find "${LOG_DIR}/snowflake" -type f -name "*.log" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true
}

log_message() {
    local level="$1"
    local message="$2"
    local timestamp
    timestamp=$(date '+%Y-%m-%dT%H:%M:%SZ')
    local hostname
    hostname=$(hostname)
    # Structured JSON log for enterprise log management systems
    local log_entry
    log_entry=$(printf '{"timestamp":"%s","script":"%s","version":"%s","session_id":"%s","user":"%s","host":"%s","level":"%s","message":"%s"}' \
        "$timestamp" "$SCRIPT_NAME" "$VERSION" "$SESSION_ID" "$USER" "$hostname" "$level" "$message")
    echo "$log_entry" | tee -a "$LOG_FILE"
}

# Function to display usage
usage() {
    cat << EOF
Usage: $SCRIPT_NAME <command> [options]

Description:
  Orchestrates ETL pipeline tasks for the blockchair-etl project. Commands include setting up environments,
  downloading data, generating Snowflake DDLs, loading data, and cleaning old files.

Commands:
  setup-env           Set up Python virtual environment and install dependencies
  download-data       Download Blockchair data to \$DATA_DIR
  generate-ddl        Generate Snowflake DDLs for data files
  load-data           Load data into Snowflake
  clean               Remove files older than \$RETENTION_DAYS (${RETENTION_DAYS})
  help                Display this help message

Environment Variables (set in config/.env):
  DATA_DIR           Directory for downloaded data [default: ${PROJECT_ROOT}/data/bitcoin]
  LOG_DIR            Directory for logs [default: ${PROJECT_ROOT}/logs]
  RETENTION_DAYS     Days to retain data and log files [default: 3]

Examples:
  $SCRIPT_NAME setup-env
  $SCRIPT_NAME download-data
  $SCRIPT_NAME generate-ddl
  $SCRIPT_NAME load-data
  $SCRIPT_NAME clean
EOF
    exit $EXIT_SUCCESS
}

# Function to validate environment variables
validate_env() {
    if [ ! -f "${PROJECT_ROOT}/config/.env" ]; then
        log_message "ERROR" "Configuration file ${PROJECT_ROOT}/config/.env not found"
        exit $EXIT_CONFIG_ERROR
    fi
    source "${PROJECT_ROOT}/config/.env"
    : "${DATA_DIR:=${PROJECT_ROOT}/data/bitcoin}"
    : "${LOG_DIR:=${PROJECT_ROOT}/logs}"
    : "${RETENTION_DAYS:=3}"
    if ! [[ "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
        log_message "ERROR" "RETENTION_DAYS must be a non-negative integer, got: $RETENTION_DAYS"
        exit $EXIT_CONFIG_ERROR
    fi
    mkdir -p "${LOG_DIR}" "${DATA_DIR}"
}

# Function to validate Python dependencies
validate_python() {
    command -v python3 >/dev/null 2>&1 || {
        log_message "ERROR" "Python3 is not installed or not in PATH"
        exit $EXIT_EXECUTION_ERROR
    }
    command -v pip3 >/dev/null 2>&1 || {
        log_message "ERROR" "pip is not installed or not in PATH"
        exit $EXIT_EXECUTION_ERROR
    }
}

# Function to set up virtual environment
setup_env() {
    log_message "INFO" "Setting up Python virtual environment"
    validate_python
    if [ -d "${PROJECT_ROOT}/.venv" ]; then
        log_message "INFO" "Virtual environment already exists, updating dependencies"
    else
        python3 -m venv "${PROJECT_ROOT}/.venv" || {
            log_message "ERROR" "Failed to create virtual environment"
            exit $EXIT_EXECUTION_ERROR
        }
    fi
    source "${PROJECT_ROOT}/.venv/bin/activate"
    pip install --upgrade pip || {
        log_message "ERROR" "Failed to upgrade pip"
        exit $EXIT_EXECUTION_ERROR
    }
    pip install pandas jsonschema colorama requests || {
        log_message "ERROR" "Failed to install Python dependencies"
        exit $EXIT_EXECUTION_ERROR
    }
    log_message "INFO" "Virtual environment setup complete"
}

# Function to download Blockchair data
download_data() {
    log_message "INFO" "Starting data download"
    validate_python
    source "${PROJECT_ROOT}/.venv/bin/activate"
    python3 "${PROJECT_ROOT}/scripts/python/download_blockchair_data.py" --log-dir "${LOG_DIR}" --skip-existing || {
        log_message "ERROR" "Data download failed"
        exit $EXIT_EXECUTION_ERROR
    }
    log_message "INFO" "Data download complete"
}

# Function to generate DDLs
generate_ddl() {
    log_message "INFO" "Generating Snowflake DDLs"
    validate_python
    source "${PROJECT_ROOT}/.venv/bin/activate"
    local data_dir="${DATA_DIR}"
    local ddl_dir="${PROJECT_ROOT}/sql/ddl"
    mkdir -p "${ddl_dir}"

    for data_type in blocks transactions inputs outputs; do
        local latest_file
        latest_file=$(ls -t "${data_dir}/${data_type}/blockchair_bitcoin_${data_type}"_*.tsv.gz 2>/dev/null | head -n 1)
        if [ -z "${latest_file}" ]; then
            log_message "WARNING" "No ${data_type} file found in ${data_dir}/${data_type}"
            continue
        fi
        local table_name="${data_type}_raw"
        local output_ddl="${ddl_dir}/create_${table_name}.sql"
        python3 "${PROJECT_ROOT}/scripts/python/generate_snowflake_ddl.py" \
            "${latest_file}" \
            "${table_name}" \
            --sample-rows 1000000 \
            --chunk-size 10000 \
            --log-dir "${LOG_DIR}" \
            --output-ddl "${output_ddl}" \
            --skip-existing || {
            log_message "ERROR" "Failed to generate DDL for ${data_type}"
            exit $EXIT_EXECUTION_ERROR
        }
        log_message "INFO" "Generated DDL for ${table_name} at ${output_ddl}"
    done
    log_message "INFO" "DDL generation complete"
}

# Function to load data into Snowflake
load_data() {
    log_message "INFO" "Starting data load to Snowflake"
    "${PROJECT_ROOT}/scripts/snowflake/load_data_to_snowflake.sh" || {
        log_message "ERROR" "Failed to load data into Snowflake"
        exit $EXIT_EXECUTION_ERROR
    }
    log_message "INFO" "Data load complete"
}

# Function to clean old files
clean() {
    log_message "INFO" "Cleaning files older than ${RETENTION_DAYS} days"
    find "${DATA_DIR}" -type f -name "*.tsv.gz" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true
    find "${LOG_DIR}" -type f -name "blockchair_etl_*.log" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true
    find "${LOG_DIR}/snowflake" -type f -name "*.log" -mtime +${RETENTION_DAYS} -delete 2>/dev/null || true
    log_message "INFO" "Cleanup complete"
}

# Main execution logic
main() {
    # Check for help option early to avoid unnecessary setup
    if [ $# -gt 0 ] && [ "$1" = "help" ]; then
        usage
    fi

    # Initialize logging
    setup_logging
    log_message "INFO" "Script started. Session ID: $SESSION_ID, User: $USER"

    # Validate environment
    validate_env

    # Check for command
    if [ $# -eq 0 ]; then
        log_message "ERROR" "No command provided"
        usage
    fi

    # Process command
    case "$1" in
        setup-env)
            setup_env
            ;;
        download-data)
            download_data
            ;;
        generate-ddl)
            generate_ddl
            ;;
        load-data)
            load_data
            ;;
        clean)
            clean
            ;;
        help)
            usage
            ;;
        *)
            log_message "ERROR" "Unknown command: $1"
            usage
            ;;
    esac

    log_message "INFO" "Script finished execution"
    exit $EXIT_SUCCESS
}

# Run main
main "$@"