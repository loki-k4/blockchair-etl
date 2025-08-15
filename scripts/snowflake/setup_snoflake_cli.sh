#!/bin/bash

# Script Name: install_snowsql.sh
# Description: Installs SnowSQL (version 1.4.4) and verifies installation and Snowflake connection.
# Version: 1.0.8
# Date: August 15, 2025
# Usage: ./install_snowsql.sh
# Dependencies: Bash 4.0+, curl, gnupg, snowsql (after installation)
# Notes:
#   - Logs timestamps in EDT (America/New_York, UTC-4) with format YYYY-MM-DDTHH:MM:SS-04:00 in JSON format.
#   - Installs SnowSQL to $HOME/bin/snowsql.
#   - Skips installation if SnowSQL version 1.4.4 is already installed and functional.
#   - Tests Snowflake connection using credentials from config/.env.
#   - Sources $LOG_DIR from config/.env for logging to $LOG_DIR/snowflake/snowsql_install_YYYYMMDD.log.

# Enable strict mode for error handling
set -euo pipefail

# Exit codes
EXIT_SUCCESS=0
EXIT_CONFIG_ERROR=1
EXIT_EXECUTION_ERROR=2

# Constants
SCRIPT_VERSION="1.0.8"
SCRIPT_NAME=$(basename "$0")
PROJECT_DIR="/root/blockchair-etl"
ENV_FILE="${PROJECT_DIR}/config/.env"
VERSION="1.4.4"
BOOTSTRAP_VERSION="1.4"
DOWNLOAD_URL="https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/${BOOTSTRAP_VERSION}/linux_x86_64/snowsql-${VERSION}-linux_x86_64.bash"
ALT_DOWNLOAD_URL="https://sfc-repo.azure.snowflakecomputing.com/snowsql/bootstrap/${BOOTSTRAP_VERSION}/linux_x86_64/snowsql-${VERSION}-linux_x86_64.bash"
SNOWSQL_BIN="${HOME}/bin/snowsql"
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

# Validate Snowflake environment variables
validate_snowflake_vars() {
    local required_vars=("SNOWSQL_ACCOUNT" "SNOWSQL_USER" "SNOWSQL_PWD" "SNOWSQL_ROLE" "SNOWSQL_DATABASE" "SNOWSQL_WAREHOUSE" "SNOWSQL_SCHEMA")
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            log_message "ERROR" "Environment variable $var is not set"
            exit $EXIT_CONFIG_ERROR
        fi
    done
}

# Check if SnowSQL is already installed and functional
check_existing_snowsql() {
    if [ -x "$SNOWSQL_BIN" ]; then
        local installed_version
        installed_version=$("$SNOWSQL_BIN" --version 2>&1 || true)
        if [[ "$installed_version" == *"Version: $VERSION"* ]]; then
            log_message "INFO" "SnowSQL version $VERSION already installed and functional at $SNOWSQL_BIN"
            return 0
        else
            log_message "WARNING" "SnowSQL found at $SNOWSQL_BIN but version is incorrect or non-functional: $installed_version"
            log_message "INFO" "Attempting to remove existing SnowSQL binary"
            if ! rm -f "$SNOWSQL_BIN"; then
                log_message "ERROR" "Failed to remove existing SnowSQL binary at $SNOWSQL_BIN. Ensure no SnowSQL processes are running."
                exit $EXIT_EXECUTION_ERROR
            fi
        fi
    fi
    return 1
}

# Test Snowflake connection
test_snowflake_connection() {
    log_message "INFO" "Testing Snowflake connection"
    if ! snowsql -a "$SNOWSQL_ACCOUNT" -u "$SNOWSQL_USER" -r "$SNOWSQL_ROLE" -w "$SNOWSQL_WAREHOUSE" -d "$SNOWSQL_DATABASE" -s "$SNOWSQL_SCHEMA" \
        --variable password="$SNOWSQL_PWD" -q "SELECT CURRENT_VERSION();" >/dev/null 2>&1; then
        log_message "ERROR" "Failed to connect to Snowflake. Check credentials in $ENV_FILE."
        exit $EXIT_EXECUTION_ERROR
    fi
    log_message "INFO" "Successfully connected to Snowflake"
}

# Main installation logic
main() {
    setup_logging
    source_env
    log_message "INFO" "Starting SnowSQL installation and connection test. Session ID: $SESSION_ID"

    # Validate Snowflake variables
    validate_snowflake_vars

    # Check for existing SnowSQL
    if check_existing_snowsql; then
        test_snowflake_connection
        log_message "INFO" "SnowSQL setup and connection test complete"
        exit $EXIT_SUCCESS
    fi

    # Install dependencies
    if ! command -v curl >/dev/null || ! command -v gpg >/dev/null; then
        log_message "INFO" "Installing dependencies (curl, gnupg)"
        if ! apt-get update || ! apt-get install -y curl gnupg; then
            log_message "ERROR" "Failed to install dependencies"
            exit $EXIT_EXECUTION_ERROR
        fi
    fi

    # Change to /tmp directory
    if ! cd /tmp; then
        log_message "ERROR" "Cannot access /tmp"
        exit $EXIT_EXECUTION_ERROR
    fi

    # Download installer
    log_message "INFO" "Downloading SnowSQL installer"
    if ! curl -f -O "$DOWNLOAD_URL"; then
        log_message "INFO" "Trying alternative Azure endpoint"
        if ! curl -f -O "$ALT_DOWNLOAD_URL"; then
            log_message "ERROR" "Failed to download SnowSQL installer"
            exit $EXIT_EXECUTION_ERROR
        fi
    fi

    # Download signature file
    log_message "INFO" "Downloading signature file"
    if ! curl -f -O "${DOWNLOAD_URL}.sig"; then
        if ! curl -f -O "${ALT_DOWNLOAD_URL}.sig"; then
            log_message "ERROR" "Failed to download signature file"
            exit $EXIT_EXECUTION_ERROR
        fi
    fi

    # Import GPG key
    log_message "INFO" "Importing Snowflake GPG key"
    if ! gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys 2A3149C82551A34A 2>/dev/null; then
        log_message "INFO" "Trying port 80 for GPG key"
        if ! gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 2A3149C82551A34A 2>/dev/null; then
            log_message "ERROR" "Failed to import GPG key"
            exit $EXIT_EXECUTION_ERROR
        fi
    fi

    # Verify installer signature
    log_message "INFO" "Verifying installer signature"
    if ! gpg --verify "snowsql-${VERSION}-linux_x86_64.bash.sig" "snowsql-${VERSION}-linux_x86_64.bash" 2>/dev/null; then
        log_message "ERROR" "Signature verification failed"
        rm -f "snowsql-${VERSION}-linux_x86_64.bash" "snowsql-${VERSION}-linux_x86_64.bash.sig"
        exit $EXIT_EXECUTION_ERROR
    fi

    # Run installer
    log_message "INFO" "Running SnowSQL installer"
    if ! echo "~/bin" | bash "snowsql-${VERSION}-linux_x86_64.bash"; then
        log_message "ERROR" "SnowSQL installation failed"
        rm -f "snowsql-${VERSION}-linux_x86_64.bash" "snowsql-${VERSION}-linux_x86_64.bash.sig"
        exit $EXIT_EXECUTION_ERROR
    fi

    rm -f "snowsql-${VERSION}-linux_x86_64.bash" "snowsql-${VERSION}-linux_x86_64.bash.sig"

    # Verify SnowSQL version
    log_message "INFO" "Verifying SnowSQL version"
    if [ ! -x "$SNOWSQL_BIN" ]; then
        log_message "ERROR" "SnowSQL not found in $SNOWSQL_BIN"
        exit $EXIT_EXECUTION_ERROR
    fi

    SNOWSQL_VERSION=$("$SNOWSQL_BIN" --version 2>&1)
    log_message "INFO" "Installed SnowSQL version: $SNOWSQL_VERSION"
    if [[ "$SNOWSQL_VERSION" != *"Version: $VERSION"* ]]; then
        log_message "ERROR" "Expected SnowSQL version $VERSION, but got: $SNOWSQL_VERSION"
        exit $EXIT_EXECUTION_ERROR
    fi

    # Test Snowflake connection
    test_snowflake_connection

    log_message "INFO" "SnowSQL installed successfully in $HOME/bin"
    log_message "INFO" "SnowSQL setup and connection test complete"
    exit $EXIT_SUCCESS
}

# Run main
main