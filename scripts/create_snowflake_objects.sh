#!/bin/bash

# Version info
VERSION="1.0.3"
SCRIPT_NAME=$(basename "$0")

# Default SnowSQL connection name and config path
SNOWSQL_CONNECTION="${SNOWSQL_CONNECTION:-blockchair}"
SNOWSQL_CONFIG="${SNOWSQL_CONFIG:-.snowsql/config}"

# Logging setup with daily rotation
LOG_DIR="logs/snowflake"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/${SCRIPT_NAME%.*}_$(date +%Y-%m-%d)_log.log"

log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local hostname=$(hostname)
    echo "$timestamp - $SCRIPT_NAME - $level - Version: $VERSION - Host: $hostname - Connection: $SNOWSQL_CONNECTION - $message" | tee -a "$LOG_FILE"
}

# Rotate logs (keep last 7 days)
find "$LOG_DIR" -name "${SCRIPT_NAME%.*}_*.log" -mtime +7 -exec rm {} \;

# Function to execute SnowSQL commands with error capture
execute_snowsql() {
    local sql_command="$1"
    local description="$2"
    local temp_sql_file
    local output
    temp_sql_file=$(mktemp /tmp/snowsql.XXXXXX.sql)
    if [ -n "$WAREHOUSE" ] && [ "$ACTION" != "warehouse" ]; then
        echo "USE WAREHOUSE $WAREHOUSE;" > "$temp_sql_file"
        echo "$sql_command" >> "$temp_sql_file"
    else
        echo "$sql_command" > "$temp_sql_file"
    fi
    log_message "DEBUG" "Executing SQL from file: $temp_sql_file"
    log_message "DEBUG" "SQL content: $(cat "$temp_sql_file")"
    output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
    local status=$?
    rm -f "$temp_sql_file"
    if [ $status -eq 0 ]; then
        log_message "INFO" "$description"
    else
        log_message "ERROR" "Failed to execute: $description. Error: $output"
        exit 1
    fi
}

# Function to check if database exists
check_database_exists() {
    local db_name="$1"
    local sql="SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.DATABASES WHERE CATALOG_NAME = UPPER('$db_name');"
    local temp_sql_file=$(mktemp /tmp/snowsql.XXXXXX.sql)
    echo "$sql" > "$temp_sql_file"
    output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
    local status=$?
    rm -f "$temp_sql_file"
    if [ $status -ne 0 ] || [ -z "$output" ]; then
        log_message "ERROR" "Database $db_name does not exist or is not accessible."
        exit 1
    fi
}

# Function to check if schema exists
check_schema_exists() {
    local db_name="$1"
    local schema_name="$2"
    local sql="SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = UPPER('$db_name') AND SCHEMA_NAME = UPPER('$schema_name');"
    local temp_sql_file=$(mktemp /tmp/snowsql.XXXXXX.sql)
    echo "USE WAREHOUSE $WAREHOUSE;" > "$temp_sql_file"
    echo "$sql" >> "$temp_sql_file"
    output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
    local status=$?
    rm -f "$temp_sql_file"
    if [ $status -ne 0 ] || [ -z "$output" ]; then
        log_message "ERROR" "Schema $db_name.$schema_name does not exist or is not accessible. Create it first using --create-schema."
        exit 1
    fi
}

# Function to extract table name from SQL file
extract_table_name() {
    local sql_file="$1"
    local sql_content
    sql_content=$(cat "$sql_file")
    table_name=$(echo "$sql_content" | grep -i "CREATE[[:space:]]\+.*TABLE" | sed -E 's/.*CREATE[[:space:]]+(OR REPLACE[[:space:]]+)?TABLE[[:space:]]+([a-zA-Z0-9_]+).*/\2/i' | head -1)
    echo "$table_name"
}

# Help message
show_help() {
    echo "Usage: $SCRIPT_NAME [options]"
    echo
    echo "This script creates Snowflake objects using SnowSQL with connection specified by -c blockchair and --config .snowsql/config."
    echo "Specify one of the following actions via arguments. Required and optional arguments for each action are listed below."
    echo "Most actions require a warehouse to be specified with --warehouse unless creating a warehouse."
    echo
    echo "Options:"
    echo "  -h, --help                Show this help message and exit"
    echo "  --version                 Show script version and exit"
    echo "  --connection NAME         SnowSQL connection name [default: blockchair]"
    echo "  --config PATH             Path to SnowSQL config file [default: .snowsql/config]"
    echo "  --warehouse NAME          Warehouse to use for operations (required for all actions except create-warehouse)"
    echo
    echo "Create Warehouse:"
    echo "   Required arguments:"
    echo "     --create-warehouse NAME          Warehouse name"
    echo "   Optional arguments:"
    echo "     --warehouse-type TYPE            Warehouse type (STANDARD, SNOWPARK-OPTIMIZED) [default: STANDARD]"
    echo "     --warehouse-size SIZE            Warehouse size (XSMALL, SMALL, MEDIUM, LARGE, XLARGE, XXLARGE) [default: XSMALL]"
    echo "     --warehouse-comment COMMENT      Comment for the warehouse [default: 'Warehouse for Bitcoin data processing']"
    echo
    echo "Create Database:"
    echo "   Required arguments:"
    echo "     --create-database NAME           Database name"
    echo "     --warehouse NAME                 Warehouse to use for the operation"
    echo "   Optional arguments:"
    echo "     --database-or-replace            Replace existing database if it exists"
    echo "     --database-transient             Create a transient database"
    echo "     --database-comment COMMENT       Comment for the database [default: 'Database for Crypto']"
    echo "     --database-tags TAGS            Tags for the database (e.g., project=blockchain,env=prod) [default: none]"
    echo
    echo "Create Schema:"
    echo "   Required arguments:"
    echo "     --create-schema NAME             Schema name"
    echo "     --database-name NAME             Database name for the schema"
    echo "     --warehouse NAME                 Warehouse to use for the operation"
    echo "   Optional arguments:"
    echo "     --schema-or-replace              Replace existing schema if it exists"
    echo "     --schema-transient               Create a transient schema"
    echo "     --schema-comment COMMENT         Comment for the schema [default: 'Schema for Bitcoin data processing']"
    echo "     --schema-tags TAGS              Tags for the schema (e.g., project=blockchain,env=prod) [default: none]"
    echo
    echo "Create File Format:"
    echo "   Required arguments:"
    echo "     --create-file-format NAME        File format name"
    echo "     --database-name NAME             Database name for the file format"
    echo "     --schema-name NAME               Schema name for the file format (must exist)"
    echo "     --warehouse NAME                 Warehouse to use for the operation"
    echo "   Optional arguments:"
    echo "     --file-format-type TYPE          File format type (CSV, JSON, PARQUET) [default: CSV]"
    echo "     --file-format-field-delimiter DELIMITER  Field delimiter for file format [default: '\t']"
    echo "     --file-format-field-optionally-enclosed-by CHAR  Field enclosure character (e.g., '\"') [default: NONE]"
    echo "     --file-format-skip-header NUM    Number of header rows to skip [default: 1]"
    echo "     --file-format-comment COMMENT    Comment for the file format [default: 'File format for TSV files used in blockchain ETL processes']"
    echo
    echo "Create Stage:"
    echo "   Required arguments:"
    echo "     --create-stage NAME              Stage name"
    echo "     --database-name NAME             Database name for the stage"
    echo "     --schema-name NAME               Schema name for the stage (must exist)"
    echo "     --warehouse NAME                 Warehouse to use for the operation"
    echo "   Optional arguments:"
    echo "     --stage-file-format NAME         File format for the stage [default: tsv_file]"
    echo "     --stage-comment COMMENT          Comment for the stage [default: 'Stage for TSV files used in blockchain ETL processes']"
    echo
    echo "Create Table (using SQL file):"
    echo "   Required arguments:"
    echo "     --create-table                   Flag to create a table (table name is extracted from the SQL file)"
    echo "     --database-name NAME             Database name for the table"
    echo "     --schema-name NAME               Schema name for the table (must exist)"
    echo "     --table-sql-path PATH            Path to .sql file containing CREATE TABLE statement"
    echo "     --warehouse NAME                 Warehouse to use for the operation"
    echo "   Optional arguments:"
    echo "     --table-name NAME                Override table name from SQL file"
    echo "     --table-file-path PATH           Path to local file to upload to stage for data loading [default: none]"
    echo "     --table-file-format NAME         File format for table data loading [default: tsv_file]"
    echo "     --table-comment COMMENT          Comment for the table [default: 'Table for blockchain data']"
    echo
    echo "Examples:"
    echo "  Create a warehouse:"
    echo "    $SCRIPT_NAME --create-warehouse MY_WAREHOUSE --warehouse-size SMALL"
    echo "  Create a database with tags:"
    echo "    $SCRIPT_NAME --create-database BITCOIN_DB --warehouse MY_WAREHOUSE --database-tags project=blockchain,env=prod"
    echo "  Create a table from SQL file in an existing schema and load data:"
    echo "    $SCRIPT_NAME --create-table --database-name BITCOIN_DB --schema-name BITCOIN_SCHEMA --table-sql-path create_table.sql --table-file-path data.tsv --warehouse MY_WAREHOUSE"
    echo "  Override table name from SQL file:"
    echo "    $SCRIPT_NAME --create-table --table-name CUSTOM_TABLE --database-name BITCOIN_DB --schema-name BITCOIN_SCHEMA --table-sql-path create_table.sql --warehouse MY_WAREHOUSE"
    echo "  Use a different connection name:"
    echo "    $SCRIPT_NAME --connection custom_connection --create-warehouse MY_WAREHOUSE"
    exit 0
}

# Check for help or version flags
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
elif [[ "$1" == "--version" ]]; then
    echo "$SCRIPT_NAME $VERSION"
    exit 0
fi

# Parse command-line arguments
CREATE_WAREHOUSE=""
WAREHOUSE_TYPE="STANDARD"
WAREHOUSE_SIZE="XSMALL"
WAREHOUSE_COMMENT="Warehouse for Bitcoin data processing"
WAREHOUSE=""
CREATE_DATABASE=""
DATABASE_OR_REPLACE=""
DATABASE_TRANSIENT=""
DATABASE_COMMENT="Database for Crypto"
DATABASE_TAGS=""
CREATE_SCHEMA=""
DATABASE_NAME=""
SCHEMA_OR_REPLACE=""
SCHEMA_TRANSIENT=""
SCHEMA_COMMENT="Schema for Bitcoin data processing"
SCHEMA_TAGS=""
CREATE_FILE_FORMAT=""
FILE_FORMAT_TYPE="CSV"
FILE_FORMAT_FIELD_DELIMITER="\t"
FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY="NONE"
FILE_FORMAT_SKIP_HEADER=1
FILE_FORMAT_COMMENT="File format for TSV files used in blockchain ETL processes"
CREATE_STAGE=""
STAGE_FILE_FORMAT="tsv_file"
STAGE_COMMENT="Stage for TSV files used in blockchain ETL processes"
CREATE_TABLE=""
TABLE_NAME=""
TABLE_SQL_PATH=""
TABLE_FILE_PATH=""
TABLE_FILE_FORMAT="tsv_file"
TABLE_COMMENT="Table for blockchain data"

# Valid values for validation
VALID_WAREHOUSE_TYPES=("STANDARD" "SNOWPARK-OPTIMIZED")
VALID_WAREHOUSE_SIZES=("XSMALL" "SMALL" "MEDIUM" "LARGE" "XLARGE" "XXLARGE")
VALID_FILE_FORMAT_TYPES=("CSV" "JSON" "PARQUET")

while [[ "$#" -gt 0 ]]; do
    case $1 in
        --create-warehouse) CREATE_WAREHOUSE="$2"; shift ;;
        --warehouse-type)
            WAREHOUSE_TYPE="$2"
            if [[ ! "${VALID_WAREHOUSE_TYPES[*]}" =~ $WAREHOUSE_TYPE ]]; then
                log_message "ERROR" "Invalid warehouse type: $WAREHOUSE_TYPE. Must be one of: ${VALID_WAREHOUSE_TYPES[*]}"
                exit 1
            fi
            shift ;;
        --warehouse-size)
            WAREHOUSE_SIZE="$2"
            if [[ ! "${VALID_WAREHOUSE_SIZES[*]}" =~ $WAREHOUSE_SIZE ]]; then
                log_message "ERROR" "Invalid warehouse size: $WAREHOUSE_SIZE. Must be one of: ${VALID_WAREHOUSE_SIZES[*]}"
                exit 1
            fi
            shift ;;
        --warehouse-comment) WAREHOUSE_COMMENT="$2"; shift ;;
        --warehouse) WAREHOUSE="$2"; shift ;;
        --connection) SNOWSQL_CONNECTION="$2"; shift ;;
        --config) SNOWSQL_CONFIG="$2"; shift ;;
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
            if [[ ! "${VALID_FILE_FORMAT_TYPES[*]}" =~ $FILE_FORMAT_TYPE ]]; then
                log_message "ERROR" "Invalid file format type: $FILE_FORMAT_TYPE. Must be one of: ${VALID_FILE_FORMAT_TYPES[*]}"
                exit 1
            fi
            shift ;;
        --file-format-field-delimiter) FILE_FORMAT_FIELD_DELIMITER="$2"; shift ;;
        --file-format-field-optionally-enclosed-by) FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY="$2"; shift ;;
        --file-format-skip-header)
            FILE_FORMAT_SKIP_HEADER="$2"
            if ! [[ "$FILE_FORMAT_SKIP_HEADER" =~ ^[0-9]+$ ]]; then
                log_message "ERROR" "Invalid skip-header value: $FILE_FORMAT_SKIP_HEADER. Must be a non-negative integer."
                exit 1
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
        --table-file-path) TABLE_FILE_PATH="$2"; shift ;;
        --table-file-format) TABLE_FILE_FORMAT="$2"; shift ;;
        --table-comment) TABLE_COMMENT="$2"; shift ;;
        *) log_message "ERROR" "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

log_message "INFO" "Script started."
log_message "INFO" "Execution started from Host: $(hostname)"
log_message "INFO" "Using SnowSQL connection: $SNOWSQL_CONNECTION, config: $SNOWSQL_CONFIG"

# Determine action based on arguments
ACTION_COUNT=0
[ -n "$CREATE_WAREHOUSE" ] && ((ACTION_COUNT++))
[ -n "$CREATE_DATABASE" ] && ((ACTION_COUNT++))
[ -n "$CREATE_SCHEMA" ] && ((ACTION_COUNT++))
[ -n "$CREATE_FILE_FORMAT" ] && ((ACTION_COUNT++))
[ -n "$CREATE_STAGE" ] && ((ACTION_COUNT++))
[ -n "$CREATE_TABLE" ] && ((ACTION_COUNT++))

if [ $ACTION_COUNT -eq 0 ]; then
    log_message "ERROR" "No action specified. Provide one of: --create-warehouse, --create-database, --create-schema, --create-file-format, --create-stage, --create-table"
    show_help
    exit 1
elif [ $ACTION_COUNT -gt 1 ]; then
    log_message "ERROR" "Multiple actions specified. Provide only one of: --create-warehouse, --create-database, --create-schema, --create-file-format, --create-stage, --create-table"
    exit 1
fi

# Set ACTION
if [ -n "$CREATE_WAREHOUSE" ]; then
    ACTION="warehouse"
elif [ -n "$CREATE_DATABASE" ]; then
    ACTION="database"
elif [ -n "$CREATE_SCHEMA" ]; then
    ACTION="schema"
elif [ -n "$CREATE_FILE_FORMAT" ]; then
    ACTION="file_format"
elif [ -n "$CREATE_STAGE" ]; then
    ACTION="stage"
elif [ -n "$CREATE_TABLE" ]; then
    ACTION="table"
fi

# Validate warehouse for actions other than create-warehouse
if [ "$ACTION" != "warehouse" ] && [ -z "$WAREHOUSE" ]; then
    log_message "ERROR" "Warehouse is required for $ACTION action. Specify --warehouse"
    exit 1
fi

# Execute based on action
case $ACTION in
    warehouse)
        if [ -z "$CREATE_WAREHOUSE" ]; then
            log_message "ERROR" "create-warehouse is required for warehouse creation"
            exit 1
        fi
        sql="CREATE OR REPLACE WAREHOUSE $CREATE_WAREHOUSE \
             WAREHOUSE_TYPE = '$WAREHOUSE_TYPE' \
             WAREHOUSE_SIZE = '$WAREHOUSE_SIZE' \
             COMMENT = '$WAREHOUSE_COMMENT'"
        execute_snowsql "$sql" "Warehouse $CREATE_WAREHOUSE created successfully."
        ;;
    database)
        if [ -z "$CREATE_DATABASE" ]; then
            log_message "ERROR" "create-database is required for database creation"
            exit 1
        fi
        TAGS=""
        if [ -n "$DATABASE_TAGS" ]; then
            TAGS="WITH TAG ($(echo "$DATABASE_TAGS" | sed 's/,/, /g'))"
        fi
        sql="CREATE ${DATABASE_OR_REPLACE}${DATABASE_TRANSIENT}DATABASE $CREATE_DATABASE \
             COMMENT = '$DATABASE_COMMENT' \
             $TAGS"
        execute_snowsql "$sql" "Database $CREATE_DATABASE created successfully."
        ;;
    schema)
        if [ -z "$CREATE_SCHEMA" ] || [ -z "$DATABASE_NAME" ]; then
            log_message "ERROR" "create-schema and database-name are required for schema creation"
            exit 1
        fi
        check_database_exists "$DATABASE_NAME"
        TAGS=""
        if [ -n "$SCHEMA_TAGS" ]; then
            TAGS="WITH TAG ($(echo "$SCHEMA_TAGS" | sed 's/,/, /g'))"
        fi
        sql="CREATE ${SCHEMA_OR_REPLACE}${SCHEMA_TRANSIENT}SCHEMA $DATABASE_NAME.$CREATE_SCHEMA \
             COMMENT = '$SCHEMA_COMMENT' \
             $TAGS"
        execute_snowsql "$sql" "Schema $DATABASE_NAME.$CREATE_SCHEMA created successfully."
        ;;
    file_format)
        if [ -z "$CREATE_FILE_FORMAT" ] || [ -z "$DATABASE_NAME" ] || [ -z "$SCHEMA_NAME" ]; then
            log_message "ERROR" "create-file-format, database-name, and schema-name are required for file format creation"
            exit 1
        fi
        check_database_exists "$DATABASE_NAME"
        check_schema_exists "$DATABASE_NAME" "$SCHEMA_NAME"
        FIELD_OPTIONALLY_ENCLOSED_BY="FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE'"
        if [ -n "$FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY" ]; then
            FIELD_OPTIONALLY_ENCLOSED_BY="FIELD_OPTIONALLY_ENCLOSED_BY = '$FILE_FORMAT_FIELD_OPTIONALLY_ENCLOSED_BY'"
        fi
        sql="CREATE OR REPLACE FILE FORMAT $DATABASE_NAME.$SCHEMA_NAME.$CREATE_FILE_FORMAT \
             TYPE = '$FILE_FORMAT_TYPE' \
             FIELD_DELIMITER = '$FILE_FORMAT_FIELD_DELIMITER' \
             $FIELD_OPTIONALLY_ENCLOSED_BY \
             SKIP_HEADER = $FILE_FORMAT_SKIP_HEADER \
             COMMENT = '$FILE_FORMAT_COMMENT'"
        execute_snowsql "$sql" "File format $DATABASE_NAME.$SCHEMA_NAME.$CREATE_FILE_FORMAT created successfully."
        ;;
    stage)
        if [ -z "$CREATE_STAGE" ] || [ -z "$DATABASE_NAME" ] || [ -z "$SCHEMA_NAME" ]; then
            log_message "ERROR" "create-stage, database-name, and schema-name are required for stage creation"
            exit 1
        fi
        check_database_exists "$DATABASE_NAME"
        check_schema_exists "$DATABASE_NAME" "$SCHEMA_NAME"
        sql="CREATE OR REPLACE STAGE $DATABASE_NAME.$SCHEMA_NAME.$CREATE_STAGE \
             FILE_FORMAT = $DATABASE_NAME.$SCHEMA_NAME.$STAGE_FILE_FORMAT \
             COMMENT = '$STAGE_COMMENT'"
        execute_snowsql "$sql" "Stage $DATABASE_NAME.$SCHEMA_NAME.$CREATE_STAGE created successfully."
        ;;
    table)
        if [ -z "$DATABASE_NAME" ] || [ -z "$SCHEMA_NAME" ] || [ -z "$TABLE_SQL_PATH" ]; then
            log_message "ERROR" "database-name, schema-name, and table-sql-path are required for table creation"
            exit 1
        fi
        if [ ! -f "$TABLE_SQL_PATH" ]; then
            log_message "ERROR" "SQL file $TABLE_SQL_PATH not found"
            exit 1
        fi
        check_database_exists "$DATABASE_NAME"
        check_schema_exists "$DATABASE_NAME" "$SCHEMA_NAME"
        if [ -z "$TABLE_NAME" ]; then
            TABLE_NAME=$(extract_table_name "$TABLE_SQL_PATH")
            if [ -z "$TABLE_NAME" ]; then
                log_message "ERROR" "Could not extract table name from $TABLE_SQL_PATH. Specify --table-name or ensure SQL file contains a valid CREATE TABLE statement."
                exit 1
            fi
            log_message "INFO" "Extracted table name '$TABLE_NAME' from SQL file $TABLE_SQL_PATH"
        fi
        TABLE_FULL_NAME="$DATABASE_NAME.$SCHEMA_NAME.$TABLE_NAME"
        sql=$(cat "$TABLE_SQL_PATH")
        sql=$(echo "$sql" | sed -E "s/CREATE[[:space:]]+(OR REPLACE[[:space:]]+)?TABLE[[:space:]]+[a-zA-Z0-9_\.]+/CREATE OR REPLACE TABLE $TABLE_FULL_NAME/i")
        if [ -n "$TABLE_COMMENT" ]; then
            if echo "$sql" | grep -q "COMMENT ="; then
                sql=$(echo "$sql" | sed "s/COMMENT = .*/COMMENT = '$TABLE_COMMENT'/")
            else
                sql="$sql COMMENT = '$TABLE_COMMENT'"
            fi
        fi
        execute_snowsql "$sql" "Table $TABLE_FULL_NAME created successfully from SQL file $TABLE_SQL_PATH."
        if [ -n "$TABLE_FILE_PATH" ]; then
            if [ ! -f "$TABLE_FILE_PATH" ]; then
                log_message "ERROR" "Data file $TABLE_FILE_PATH not found"
                exit 1
            fi
            temp_sql_file=$(mktemp /tmp/snowsql.XXXXXX.sql)
            echo "USE WAREHOUSE $WAREHOUSE;" > "$temp_sql_file"
            echo "PUT file://$TABLE_FILE_PATH @%$TABLE_FULL_NAME;" >> "$temp_sql_file"
            output=$(snowsql -c "$SNOWSQL_CONNECTION" --config "$SNOWSQL_CONFIG" -o quiet=true -f "$temp_sql_file" 2>&1)
            local status=$?
            rm -f "$temp_sql_file"
            if [ $status -eq 0 ]; then
                log_message "INFO" "File $TABLE_FILE_PATH uploaded to table stage @%$TABLE_FULL_NAME."
            else
                log_message "ERROR" "Failed to upload file $TABLE_FILE_PATH to table stage. Error: $output"
                exit 1
            fi
            sql="COPY INTO $TABLE_FULL_NAME \
                 FROM @%$TABLE_FULL_NAME \
                 FILE_FORMAT = (FORMAT_NAME = $DATABASE_NAME.$SCHEMA_NAME.$TABLE_FILE_FORMAT);"
            execute_snowsql "$sql" "Data loaded into table $TABLE_FULL_NAME from file $TABLE_FILE_PATH."
        fi
        ;;
esac

log_message "INFO" "Script finished execution."