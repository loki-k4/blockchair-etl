#!/bin/bash

# Script Name: snowflake_manager.sh
# Description: A robust script for managing Snowflake objects (warehouses, databases, schemas, file formats, stages, tables)
#              using SnowSQL. Designed for enterprise use with enhanced logging, validation, and error handling.
# Version: 1.1.2
# Author: [Your Company/Author Name]
# Date: August 14, 2025
# Usage: ./snowflake_manager.sh [options]
# Dependencies: SnowSQL CLI, Bash 4.0+

# Exit codes
EXIT_SUCCESS=0
EXIT_INVALID_ARGS=1
EXIT_CONFIG_ERROR=2
EXIT_SNOWSQL_ERROR=3
EXIT_FILE_NOT_FOUND=4
EXIT_VALIDATION_ERROR=5

# Default configuration
VERSION="1.1.2"
SCRIPT_NAME=$(basename "$0")
LOG_DIR="logs/snowflake"
SNOWSQL_CONNECTION="blockchair"
SNOWSQL_CONFIG="config/snowsql_config.ini"
SESSION_ID=$(uuidgen || cat /proc/sys/kernel/random/uuid)
USER=$(whoami)

# Valid values for validation
VALID_WAREHOUSE_TYPES=("STANDARD" "SNOWPARK-OPTIMIZED")
VALID_WAREHOUSE_SIZES=("XSMALL" "SMALL" "MEDIUM" "LARGE" "XLARGE" "XXLARGE")
VALID_FILE_FORMAT_TYPES=("CSV" "JSON" "PARQUET")

# Logging setup with structured JSON output
setup_logging() {
    mkdir -p "$LOG_DIR"
    LOG_FILE="$LOG_DIR/${SCRIPT_NAME%.*}_$(date +%Y-%m-%d).log"
    # Rotate logs (keep last 7 days)
    find "$LOG_DIR" -name "${SCRIPT_NAME%.*}_*.log" -mtime +7 -exec rm {} \; 2>/dev/null
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
    log_entry=$(printf '{"timestamp":"%s","script":"%s","version":"%s","session_id":"%s","user":"%s","host":"%s","connection":"%s","level":"%s","message":"%s"}' \
        "$timestamp" "$SCRIPT_NAME" "$VERSION" "$SESSION_ID" "$USER" "$hostname" "$SNOWSQL_CONNECTION" "$level" "$message")
    echo "$log_entry" | tee -a "$LOG_FILE"
}

# Securely create and clean up temporary SQL file
create_temp_sql_file() {
    local temp_file
    temp_file=$(mktemp /tmp/snowsql.XXXXXX.sql) || {
        log_message "ERROR" "Failed to create temporary file"
        exit $EXIT_CONFIG_ERROR
    }
    echo "$temp_file"
}

# Execute SnowSQL commands with error handling
execute_snowsql() {
    local sql_command="$1"
    local description="$2"
    local temp_sql_file
    temp_sql_file=$(create_temp_sql_file)
    if [[ -n "$WAREHOUSE" && "$ACTION" != "warehouse" ]]; then
        echo "USE WAREHOUSE $WAREHOUSE;" > "$temp_sql_file"
    fi
    echo "$sql_command" >> "$temp_sql_file"
    log_message "DEBUG" "Executing SQL: $description (file: $temp_sql_file)"
    local output
    output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
    local status=$?
    rm -f "$temp_sql_file"
    if [[ $status -eq 0 ]]; then
        log_message "INFO" "$description"
    else
        log_message "ERROR" "Failed to execute: $description. Error: $output"
        exit $EXIT_SNOWSQL_ERROR
    fi
}

# Validate database existence
check_database_exists() {
    local db_name="$1"
    local sql="SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.DATABASES WHERE CATALOG_NAME = UPPER('$db_name');"
    local temp_sql_file
    temp_sql_file=$(create_temp_sql_file)
    echo "$sql" > "$temp_sql_file"
    local output
    output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
    local status=$?
    rm -f "$temp_sql_file"
    if [[ $status -ne 0 || -z "$output" ]]; then
        log_message "ERROR" "Database $db_name does not exist or is not accessible"
        exit $EXIT_VALIDATION_ERROR
    fi
}

# Validate schema existence
check_schema_exists() {
    local db_name="$1"
    local schema_name="$2"
    local sql="SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = UPPER('$db_name') AND SCHEMA_NAME = UPPER('$schema_name');"
    local temp_sql_file
    temp_sql_file=$(create_temp_sql_file)
    echo "USE WAREHOUSE $WAREHOUSE;" > "$temp_sql_file"
    echo "$sql" >> "$temp_sql_file"
    local output
    output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
    local status=$?
    rm -f "$temp_sql_file"
    if [[ $status -ne 0 || -z "$output" ]]; then
        log_message "ERROR" "Schema $db_name.$schema_name does not exist or is not accessible"
        exit $EXIT_VALIDATION_ERROR
    fi
}

# Extract table name from SQL file
extract_table_name() {
    local sql_file="$1"
    if [[ ! -f "$sql_file" ]]; then
        log_message "ERROR" "SQL file $sql_file not found"
        exit $EXIT_FILE_NOT_FOUND
    fi
    local sql_content
    sql_content=$(cat "$sql_file")
    local table_name
    table_name=$(echo "$sql_content" | grep -i "CREATE[[:space:]]\+.*TABLE" | sed -E 's/.*CREATE[[:space:]]+(OR REPLACE[[:space:]]+)?TABLE[[:space:]]+([a-zA-Z0-9_]+).*/\2/i' | head -1)
    if [[ -z "$table_name" ]]; then
        log_message "ERROR" "Could not extract table name from $sql_file"
        exit $EXIT_VALIDATION_ERROR
    fi
    echo "$table_name"
}

# Display help message
show_help() {
    cat << EOF
Usage: $SCRIPT_NAME [options]

Description:
  Manages Snowflake objects (warehouses, databases, schemas, file formats, stages, tables) using SnowSQL.
  Supports a single action per execution. All actions except --create-warehouse require --warehouse.

Options:
  -h, --help                Display this help message and exit
  --version                 Display script version and exit
  --connection NAME         SnowSQL connection name [default: $SNOWSQL_CONNECTION]
  --warehouse NAME          Warehouse for operations (required except for --create-warehouse)

Create Warehouse:
  --create-warehouse NAME   Warehouse name
  --warehouse-type TYPE     Warehouse type (${VALID_WAREHOUSE_TYPES[*]}) [default: STANDARD]
  --warehouse-size SIZE     Warehouse size (${VALID_WAREHOUSE_SIZES[*]}) [default: XSMALL]
  --warehouse-comment TEXT  Comment [default: 'Warehouse for data processing']

Create Database:
  --create-database NAME    Database name
  --database-or-replace     Replace existing database
  --database-transient      Create transient database
  --database-comment TEXT   Comment [default: 'Database for data processing']
  --database-tags TAGS      Tags (e.g., project=blockchain,env=prod)

Create Schema:
  --create-schema NAME      Schema name
  --database-name NAME      Database name
  --schema-or-replace       Replace existing schema
  --schema-transient        Create transient schema
  --schema-comment TEXT     Comment [default: 'Schema for data processing']
  --schema-tags TAGS        Tags (e.g., project=blockchain,env=prod)

Create File Format:
  --create-file-format NAME File format name
  --database-name NAME      Database name
  --schema-name NAME        Schema name
  --file-format-type TYPE   File format type (${VALID_FILE_FORMAT_TYPES[*]}) [default: CSV]
  --file-format-field-delimiter CHAR  Delimiter [default: '\t']
  --file-format-field-optionally-enclosed-by CHAR  Enclosure [default: NONE]
  --file-format-skip-header NUM  Header rows to skip [default: 1]
  --file-format-comment TEXT  Comment [default: 'File format for ETL processes']

Create Stage:
  --create-stage NAME       Stage name
  --database-name NAME      Database name
  --schema-name NAME        Schema name
  --stage-file-format NAME  File format [default: tsv_file]
  --stage-comment TEXT      Comment [default: 'Stage for ETL processes']

Create Table:
  --create-table            Create table from SQL file
  --database-name NAME      Database name
  --schema-name NAME        Schema name
  --table-sql-path PATH     Path to CREATE TABLE SQL file
  --table-name NAME         Override table name from SQL file
  --table-comment TEXT      Comment [default: 'Table for data']

Examples:
  Create warehouse: $SCRIPT_NAME --create-warehouse MY_WH --warehouse-size SMALL
  Create database: $SCRIPT_NAME --create-database MY_DB --warehouse MY_WH --database-tags project=data
  Create table: $SCRIPT_NAME --create-table --database-name MY_DB --schema-name MY_SCHEMA --table-sql-path table.sql --warehouse MY_WH
EOF
    exit $EXIT_SUCCESS
}

# Validate required arguments for each action
validate_arguments() {
    case $ACTION in
        warehouse)
            [[ -z "$CREATE_WAREHOUSE" ]] && { log_message "ERROR" "Missing --create-warehouse"; exit $EXIT_INVALID_ARGS; }
            ;;
        database)
            [[ -z "$CREATE_DATABASE" ]] && { log_message "ERROR" "Missing --create-database"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$WAREHOUSE" ]] && { log_message "ERROR" "Missing --warehouse"; exit $EXIT_INVALID_ARGS; }
            ;;
        schema)
            [[ -z "$CREATE_SCHEMA" ]] && { log_message "ERROR" "Missing --create-schema"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$DATABASE_NAME" ]] && { log_message "ERROR" "Missing --database-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$WAREHOUSE" ]] && { log_message "ERROR" "Missing --warehouse"; exit $EXIT_INVALID_ARGS; }
            ;;
        file_format)
            [[ -z "$CREATE_FILE_FORMAT" ]] && { log_message "ERROR" "Missing --create-file-format"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$DATABASE_NAME" ]] && { log_message "ERROR" "Missing --database-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$SCHEMA_NAME" ]] && { log_message "ERROR" "Missing --schema-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$WAREHOUSE" ]] && { log_message "ERROR" "Missing --warehouse"; exit $EXIT_INVALID_ARGS; }
            ;;
        stage)
            [[ -z "$CREATE_STAGE" ]] && { log_message "ERROR" "Missing --create-stage"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$DATABASE_NAME" ]] && { log_message "ERROR" "Missing --database-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$SCHEMA_NAME" ]] && { log_message "ERROR" "Missing --schema-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$WAREHOUSE" ]] && { log_message "ERROR" "Missing --warehouse"; exit $EXIT_INVALID_ARGS; }
            ;;
        table)
            [[ -z "$DATABASE_NAME" ]] && { log_message "ERROR" "Missing --database-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$SCHEMA_NAME" ]] && { log_message "ERROR" "Missing --schema-name"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$TABLE_SQL_PATH" ]] && { log_message "ERROR" "Missing --table-sql-path"; exit $EXIT_INVALID_ARGS; }
            [[ -z "$WAREHOUSE" ]] && { log_message "ERROR" "Missing --warehouse"; exit $EXIT_INVALID_ARGS; }
            ;;
    esac
}

# Parse command-line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help) show_help ;;
            --version) echo "$SCRIPT_NAME $VERSION"; exit $EXIT_SUCCESS ;;
            --connection) SNOWSQL_CONNECTION="$2"; shift ;;
            --warehouse) WAREHOUSE="$2"; shift ;;
            --create-warehouse) CREATE_WAREHOUSE="$2"; shift ;;
            --warehouse-type)
                WAREHOUSE_TYPE="$2"
                [[ ! "${VALID_WAREHOUSE_TYPES[*]}" =~ $WAREHOUSE_TYPE ]] && {
                    log_message "ERROR" "Invalid warehouse type: $WAREHOUSE_TYPE. Must be one of: ${VALID_WAREHOUSE_TYPES[*]}"
                    exit $EXIT_INVALID_ARGS
                }
                shift ;;
            --warehouse-size)
                WAREHOUSE_SIZE="$2"
                [[ ! "${VALID_WAREHOUSE_SIZES[*]}" =~ $WAREHOUSE_SIZE ]] && {
                    log_message "ERROR" "Invalid warehouse size: $WAREHOUSE_SIZE. Must be one of: ${VALID_WAREHOUSE_SIZES[*]}"
                    exit $EXIT_INVALID_ARGS
                }
                shift ;;
            --warehouse-comment) WAREHOUSE_COMMENT="$2"; shift ;;
            --create-database) CREATE_DATABASE="$2"; shift ;;
            --database-or-replace) DATABASE_OR_REPLACE="OR REPLACE "; ;;
            --database-transient) DATABASE_TRANSIENT="TRANSIENT "; ;;
            --database-comment) DATABASE_COMMENT="$2"; shift ;;
            --database-tags) DATABASE_TAGS="$2"; shift ;;
            --create-schema) CREATE_SCHEMA="$2"; shift ;;
            --database-name) DATABASE_NAME="$2"; shift ;;
            --schema-or-replace) SCHEMA_OR_REPLACE="OR REPLACE "; ;;
            --schema-transient) SCHEMA_TRANSIENT="TRANSIENT "; ;;
            --schema-comment) SCHEMA_COMMENT="$2"; shift ;;
            --schema-tags) SCHEMA_TAGS="$2"; shift ;;
            --create-file-format) CREATE_FILE_FORMAT="$2"; shift ;;
            --file-format-type)
                FILE_FORMAT_TYPE="$2"
                [[ ! "${VALID_FILE_FORMAT_TYPES[*]}" =~ $FILE_FORMAT_TYPE ]] && {
                    log_message "ERROR" "Invalid file format type: $FILE_FORMAT_TYPE. Must be one of: ${VALID_FILE_FORMAT_TYPES[*]}"
                    exit $EXIT_INVALID_ARGS
                }
                shift ;;
            --file-format-field-delimiter) FILE_FORMAT_FIELD_DELIMITER="$2"; shift ;;
            --file-format-field-optionally-enclosed-by) FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY="$2"; shift ;;
            --file-format-skip-header)
                FILE_FORMAT_SKIP_HEADER="$2"
                if ! [[ "$FILE_FORMAT_SKIP_HEADER" =~ ^[0-9]+$ ]]; then
                    log_message "ERROR" "Invalid skip-header value: $FILE_FORMAT_SKIP_HEADER. Must be a non-negative integer."
                    exit $EXIT_INVALID_ARGS
                fi
                shift ;;
            --file-format-comment) FILE_FORMAT_COMMENT="$2"; shift ;;
            --create-stage) CREATE_STAGE="$2"; shift ;;
            --stage-file-format) STAGE_FILE_FORMAT="$2"; shift ;;
            --stage-comment) STAGE_COMMENT="$2"; shift ;;
            --create-table) CREATE_TABLE="true"; ;;
            --table-name) TABLE_NAME="$2"; shift ;;
            --schema-name) SCHEMA_NAME="$2"; shift ;;
            --table-sql-path) TABLE_SQL_PATH="$2"; shift ;;
            --table-comment) TABLE_COMMENT="$2"; shift ;;
            *) log_message "ERROR" "Unknown parameter: $1"; exit $EXIT_INVALID_ARGS ;;
        esac
        shift
    done
}

# Main execution logic
main() {
    # Initialize logging
    setup_logging
    log_message "INFO" "Script started. Session ID: $SESSION_ID, User: $USER"

    # Parse arguments
    parse_arguments "$@"

    # Initialize variables with defaults
    WAREHOUSE_TYPE=${WAREHOUSE_TYPE:-"STANDARD"}
    WAREHOUSE_SIZE=${WAREHOUSE_SIZE:-"XSMALL"}
    WAREHOUSE_COMMENT=${WAREHOUSE_COMMENT:-"Warehouse for data processing"}
    DATABASE_COMMENT=${DATABASE_COMMENT:-"Database for data processing"}
    SCHEMA_COMMENT=${SCHEMA_COMMENT:-"Schema for data processing"}
    FILE_FORMAT_TYPE=${FILE_FORMAT_TYPE:-"CSV"}
    FILE_FORMAT_FIELD_DELIMITER=${FILE_FORMAT_FIELD_DELIMITER:-"\t"}
    FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY=${FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY:-"NONE"}
    FILE_FORMAT_SKIP_HEADER=${FILE_FORMAT_SKIP_HEADER:-1}
    FILE_FORMAT_COMMENT=${FILE_FORMAT_COMMENT:-"File format for ETL processes"}
    STAGE_FILE_FORMAT=${STAGE_FILE_FORMAT:-"tsv_file"}
    STAGE_COMMENT=${STAGE_COMMENT:-"Stage for ETL processes"}
    TABLE_COMMENT=${TABLE_COMMENT:-"Table for data"}

    # Determine action
    ACTION_COUNT=0
    [[ -n "$CREATE_WAREHOUSE" ]] && ((ACTION_COUNT++))
    [[ -n "$CREATE_DATABASE" ]] && ((ACTION_COUNT++))
    [[ -n "$CREATE_SCHEMA" ]] && ((ACTION_COUNT++))
    [[ -n "$CREATE_FILE_FORMAT" ]] && ((ACTION_COUNT++))
    [[ -n "$CREATE_STAGE" ]] && ((ACTION_COUNT++))
    [[ -n "$CREATE_TABLE" ]] && ((ACTION_COUNT++))

    if [[ $ACTION_COUNT -eq 0 ]]; then
        log_message "ERROR" "No action specified. Provide one of: --create-warehouse, --create-database, --create-schema, --create-file-format, --create-stage, --create-table"
        show_help
    elif [[ $ACTION_COUNT -gt 1 ]]; then
        log_message "ERROR" "Multiple actions specified. Only one action is allowed per execution."
        exit $EXIT_INVALID_ARGS
    fi

    # Set action
    if [[ -n "$CREATE_WAREHOUSE" ]]; then
        ACTION="warehouse"
    elif [[ -n "$CREATE_DATABASE" ]]; then
        ACTION="database"
    elif [[ -n "$CREATE_SCHEMA" ]]; then
        ACTION="schema"
    elif [[ -n "$CREATE_FILE_FORMAT" ]]; then
        ACTION="file_format"
    elif [[ -n "$CREATE_STAGE" ]]; then
        ACTION="stage"
    elif [[ -n "$CREATE_TABLE" ]]; then
        ACTION="table"
    fi

    # Validate arguments for the selected action
    validate_arguments

    # Execute action
    case $ACTION in
        warehouse)
            sql="CREATE OR REPLACE WAREHOUSE $CREATE_WAREHOUSE \
                 WAREHOUSE_TYPE = '$WAREHOUSE_TYPE' \
                 WAREHOUSE_SIZE = '$WAREHOUSE_SIZE' \
                 COMMENT = '$WAREHOUSE_COMMENT'"
            execute_snowsql "$sql" "Warehouse $CREATE_WAREHOUSE created successfully."
            ;;
        database)
            TAGS=""
            [[ -n "$DATABASE_TAGS" ]] && TAGS="WITH TAG ($(echo "$DATABASE_TAGS" | sed 's/,/, /g'))"
            sql="CREATE ${DATABASE_OR_REPLACE}${DATABASE_TRANSIENT}DATABASE $CREATE_DATABASE \
                 COMMENT = '$DATABASE_COMMENT' \
                 $TAGS"
            execute_snowsql "$sql" "Database $CREATE_DATABASE created successfully."
            ;;
        schema)
            check_database_exists "$DATABASE_NAME"
            TAGS=""
            [[ -n "$SCHEMA_TAGS" ]] && TAGS="WITH TAG ($(echo "$SCHEMA_TAGS" | sed 's/,/, /g'))"
            sql="CREATE ${SCHEMA_OR_REPLACE}${SCHEMA_TRANSIENT}SCHEMA $DATABASE_NAME.$CREATE_SCHEMA \
                 COMMENT = '$SCHEMA_COMMENT' \
                 $TAGS"
            execute_snowsql "$sql" "Schema $DATABASE_NAME.$CREATE_SCHEMA created successfully."
            ;;
        file_format)
            check_database_exists "$DATABASE_NAME"
            check_schema_exists "$DATABASE_NAME" "$SCHEMA_NAME"
            FIELD_OPTIONALLY_ENCLOSED_BY="FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'"
            [[ "$FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY" != "NONE" ]] && \
                FIELD_OPTIONALLY_ENCLOSED_BY="FIELD_OPTIONALLY_ENCLOSED_BY = '$FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY'"
            sql="CREATE OR REPLACE FILE FORMAT $DATABASE_NAME.$SCHEMA_NAME.$CREATE_FILE_FORMAT \
                 TYPE = '$FILE_FORMAT_TYPE' \
                 FIELD_DELIMITER = '$FILE_FORMAT_FIELD_DELIMITER' \
                 $FIELD_OPTIONALLY_ENCLOSED_BY \
                 SKIP_HEADER = $FILE_FORMAT_SKIP_HEADER \
                 COMMENT = '$FILE_FORMAT_COMMENT'"
            execute_snowsql "$sql" "File format $DATABASE_NAME.$SCHEMA_NAME.$CREATE_FILE_FORMAT created successfully."
            ;;
        stage)
            check_database_exists "$DATABASE_NAME"
            check_schema_exists "$DATABASE_NAME" "$SCHEMA_NAME"
            sql="CREATE OR REPLACE STAGE $DATABASE_NAME.$SCHEMA_NAME.$CREATE_STAGE \
                 FILE_FORMAT = $DATABASE_NAME.$SCHEMA_NAME.$STAGE_FILE_FORMAT \
                 COMMENT = '$STAGE_COMMENT'"
            execute_snowsql "$sql" "Stage $DATABASE_NAME.$SCHEMA_NAME.$CREATE_STAGE created successfully."
            ;;
        table)
            check_database_exists "$DATABASE_NAME"
            check_schema_exists "$DATABASE_NAME" "$SCHEMA_NAME"
            if [[ ! -f "$TABLE_SQL_PATH" ]]; then
                log_message "ERROR" "SQL file $TABLE_SQL_PATH not found"
                exit $EXIT_FILE_NOT_FOUND
            fi
            if [[ -z "$TABLE_NAME" ]]; then
                TABLE_NAME=$(extract_table_name "$TABLE_SQL_PATH")
                log_message "INFO" "Extracted table name '$TABLE_NAME' from SQL file $TABLE_SQL_PATH"
            fi
            TABLE_FULL_NAME="$DATABASE_NAME.$SCHEMA_NAME.$TABLE_NAME"
            sql=$(cat "$TABLE_SQL_PATH")
            sql=$(echo "$sql" | sed -E "s/CREATE[[:space:]]+(OR REPLACE[[:space:]]+)?TABLE[[:space:]]+[a-zA-Z0-9_\.]+/CREATE OR REPLACE TABLE $TABLE_FULL_NAME/i")
            if [[ -n "$TABLE_COMMENT" ]]; then
                if echo "$sql" | grep -q "COMMENT ="; then
                    sql=$(echo "$sql" | sed "s/COMMENT = .*/COMMENT = '$TABLE_COMMENT'/")
                else
                    sql="$sql COMMENT = '$TABLE_COMMENT'"
                fi
            fi
            execute_snowsql "$sql" "Table $TABLE_FULL_NAME created successfully from SQL file $TABLE_SQL_PATH."
            ;;
    esac

    log_message "INFO" "Script finished execution."
}

# Run main
main "$@"