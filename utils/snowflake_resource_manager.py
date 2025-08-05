import argparse
import logging
import logging.handlers
import snowflake.connector
from snowflake.connector import DictCursor
import os
import socket
from datetime import datetime
from dotenv import load_dotenv

# Version info
__version__ = '1.0.0'
SCRIPT_NAME = os.path.basename(__file__)

# Load environment variables from .env file
load_dotenv()

# Snowflake connection parameters from .env
snowflake_config = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')  # Default to ACCOUNTADMIN
}

# Validate environment variables
for key, value in snowflake_config.items():
    if value is None:
        raise ValueError(f"Missing required environment variable for {key}")

# Custom Formatter to handle missing attributes
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'program'):
            record.program = 'unknown'
        if not hasattr(record, 'version'):
            record.version = 'unknown'
        if not hasattr(record, 'hostname'):
            record.hostname = 'unknown'
        return super().format(record)

# Corporate-Level Logging Setup with Daily Log Rotation
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    log_directory = "logs/snowflake"
    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(log_directory, f"{SCRIPT_NAME.split('.')[0]}_{datetime.now().strftime('%Y-%m-%d')}_log.log")

    handler = logging.handlers.TimedRotatingFileHandler(
        log_filename, when="midnight", interval=1, backupCount=7
    )
    handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    log_format = (
        '%(asctime)s - %(name)s - %(levelname)s - '
        'Script: %(program)s - Version: %(version)s - '
        'Host: %(hostname)s - %(message)s'
    )
    formatter = CustomFormatter(log_format)
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.addHandler(console_handler)

    return logging.LoggerAdapter(logger, {'program': SCRIPT_NAME, 'version': __version__, 'hostname': socket.gethostname()})

def create_warehouse(cursor, logger, **args):
    """Create a Snowflake warehouse with dynamic naming."""
    sql_command = f"""
        CREATE OR REPLACE WAREHOUSE {args['create_warehouse']}
            WAREHOUSE_TYPE = '{args['warehouse_type']}'
            WAREHOUSE_SIZE = '{args['warehouse_size']}'
            COMMENT = '{args['comment']}'
    """
    try:
        cursor.execute(sql_command)
        logger.info(f"Warehouse {args['create_warehouse']} created successfully.")
    except Exception as e:
        logger.error(f"Error creating warehouse {args['create_warehouse']}: {e}")
        raise

def build_tags_string(tags_dict):
    """Helper function to build the Snowflake tags string."""
    if tags_dict:
        return "WITH TAG (" + ", ".join([f"{k} = '{v}'" for k, v in tags_dict.items()]) + ")"
    return ''

def create_database(cursor, logger, **args):
    """Create a Snowflake database with dynamic parameters."""
    or_replace = 'OR REPLACE ' if args.get('or_replace', False) else ''
    transient = 'TRANSIENT ' if args.get('transient', False) else ''
    comment = f"COMMENT = '{args['comment']}'" if args.get('comment') else ''
    tags = build_tags_string(args.get('tags', {}))

    sql_command = f"""
        CREATE {or_replace}{transient}DATABASE {args['database_name']}
        {comment}
        {tags}
    """.strip()

    try:
        cursor.execute(sql_command)
        logger.info(f"Database {args['database_name']} created successfully.")
    except Exception as e:
        logger.error(f"Error creating database {args['database_name']}: {e}")
        raise

def create_schema(cursor, logger, **args):
    """Create a Snowflake schema with dynamic parameters."""
    or_replace = 'OR REPLACE ' if args.get('or_replace', False) else ''
    transient = 'TRANSIENT ' if args.get('transient', False) else ''
    comment = f"COMMENT = '{args['comment']}'" if args.get('comment') else ''
    tags = build_tags_string(args.get('tags', {}))

    if not args.get('database_name'):
        raise ValueError("database_name is required for schema creation")

    sql_command = f"""
        CREATE {or_replace}{transient}SCHEMA {args['database_name']}.{args['schema_name']}
        {comment}
        {tags}
    """.strip()

    try:
        cursor.execute(sql_command)
        logger.info(f"Schema {args['database_name']}.{args['schema_name']} created successfully.")
    except Exception as e:
        logger.error(f"Error creating schema {args['database_name']}.{args['schema_name']}: {e}")
        raise

def create_file_format(cursor, logger, **args):
    """Create a Snowflake file format with dynamic parameters."""
    field_optionally_enclosed_by = f"FIELD_OPTIONALLY_ENCLOSED_BY = '{args['field_optionally_enclosed_by']}'" if args.get('field_optionally_enclosed_by') else 'FIELD_OPTIONALLY_ENCLOSED_BY = NONE'
    comment = f"COMMENT = '{args['comment']}'" if args.get('comment') else ''

    if not args.get('database_name') or not args.get('schema_name'):
        raise ValueError("database_name and schema_name are required for file format creation")

    sql_command = f"""
        CREATE OR REPLACE FILE FORMAT {args['database_name']}.{args['schema_name']}.{args['file_format_name']}
            TYPE = '{args['type']}'
            FIELD_DELIMITER = '{args['field_delimiter']}'
            {field_optionally_enclosed_by}
            SKIP_HEADER = {args['skip_header']}
            {comment}
    """.strip()

    try:
        cursor.execute(sql_command)
        logger.info(f"File format {args['database_name']}.{args['schema_name']}.{args['file_format_name']} created successfully.")
    except Exception as e:
        logger.error(f"Error creating file format {args['database_name']}.{args['schema_name']}.{args['file_format_name']}: {e}")
        raise

def create_stage(cursor, logger, **args):
    """Create a Snowflake stage with dynamic parameters."""
    comment = f"COMMENT = '{args['comment']}'" if args.get('comment') else ''

    if not args.get('database_name') or not args.get('schema_name'):
        raise ValueError("database_name and schema_name are required for stage creation")

    sql_command = f"""
        CREATE OR REPLACE STAGE {args['database_name']}.{args['schema_name']}.{args['stage_name']}
            FILE_FORMAT = {args['database_name']}.{args['schema_name']}.{args['file_format']}
            {comment}
    """.strip()

    try:
        cursor.execute(sql_command)
        logger.info(f"Stage {args['database_name']}.{args['schema_name']}.{args['stage_name']} created successfully.")
    except Exception as e:
        logger.error(f"Error creating stage {args['database_name']}.{args['schema_name']}.{args['stage_name']}: {e}")
        raise

def create_table(cursor, logger, **args):
    """Create a Snowflake table using a SQL statement from a .sql file and load data from a stage."""
    if not args.get('database_name') or not args.get('schema_name'):
        raise ValueError("database_name and schema_name are required for table creation")
    if not args.get('table_sql_path'):
        raise ValueError("table-sql-path is required for table creation")

    # Read the SQL file
    try:
        with open(args['table_sql_path'], 'r') as file:
            sql_create_table = file.read().strip()
    except Exception as e:
        logger.error(f"Error reading SQL file {args['table_sql_path']}: {e}")
        raise

    # Ensure the table name in the SQL is fully qualified
    table_name = f"{args['database_name']}.{args['schema_name']}.{args['table_name']}"
    sql_create_table = sql_create_table.replace(
        f"CREATE OR REPLACE TABLE {args['table_name']}",
        f"CREATE OR REPLACE TABLE {table_name}"
    ).replace(
        f"CREATE TABLE {args['table_name']}",
        f"CREATE OR REPLACE TABLE {table_name}"
    )

    # Append or replace comment if provided
    if args.get('comment'):
        if 'COMMENT =' in sql_create_table:
            sql_create_table = sql_create_table.replace(
                r"COMMENT = '.*?'",
                f"COMMENT = '{args['comment']}'"
            )
        else:
            sql_create_table = f"{sql_create_table} COMMENT = '{args['comment']}'"

    try:
        cursor.execute(sql_create_table)
        logger.info(f"Table {table_name} created successfully from SQL file {args['table_sql_path']}.")
    except Exception as e:
        logger.error(f"Error creating table {table_name}: {e}")
        raise

    # Load data from stage if file_path is provided
    if args.get('file_path'):
        sql_copy_into = f"""
            COPY INTO {table_name}
            FROM @{args['database_name']}.{args['schema_name']}.{args['file_format']}/{args['file_path']}
            FILE_FORMAT = (FORMAT_NAME = {args['database_name']}.{args['schema_name']}.{args['file_format']})
        """.strip()

        try:
            cursor.execute(sql_copy_into)
            logger.info(f"Data loaded into table {table_name} from stage path {args['file_path']}.")
        except Exception as e:
            logger.error(f"Error loading data into table {table_name}: {e}")
            raise

def main():
    parser = argparse.ArgumentParser(description="Create a Snowflake warehouse, database, schema, file format, stage, or table")
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')

    # Warehouse arguments
    warehouse_args = parser.add_argument_group('Warehouse Arguments')
    warehouse_args.add_argument('--create-warehouse', type=str, help="Warehouse name")
    warehouse_args.add_argument('--warehouse-type', choices=['STANDARD', 'SNOWPARK-OPTIMIZED'], default='STANDARD', help="Warehouse type")
    warehouse_args.add_argument('--warehouse-size', choices=['XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'XLARGE', 'XXLARGE'], default='XSMALL', help="Warehouse size")
    warehouse_args.add_argument('--warehouse-comment', type=str, default='Warehouse for Bitcoin data processing', help="Comment for the warehouse")

    # Database arguments
    database_args = parser.add_argument_group('Database Arguments')
    database_args.add_argument('--create-database', type=str, dest='database_name', help="Database name")
    database_args.add_argument('--database-or-replace', action='store_true', help="Replace existing database")
    database_args.add_argument('--database-transient', action='store_true', help="Create a transient database")
    database_args.add_argument('--database-comment', type=str, default='Database for Crypto', help="Comment for the database")
    database_args.add_argument('--database-tags', type=lambda s: dict(item.split('=') for item in s.split(',') if '=' in item), default={}, help="Tags for the database (e.g., project=blockchain,env=prod)")

    # Schema arguments (includes file format, stage, and table)
    schema_args = parser.add_argument_group('Schema Arguments')
    schema_args.add_argument('--create-schema', type=str, dest='schema_name', help="Schema name")
    schema_args.add_argument('--schema-database-name', type=str, dest='database_name', help="Database name for schema, file format, stage, or table creation")
    schema_args.add_argument('--schema-or-replace', action='store_true', help="Replace existing schema")
    schema_args.add_argument('--schema-transient', action='store_true', help="Create a transient schema")
    schema_args.add_argument('--schema-comment', type=str, default='Schema for Bitcoin data processing', help="Comment for the schema")
    schema_args.add_argument('--schema-tags', type=lambda s: dict(item.split('=') for item in s.split(',') if '=' in item), default={}, help="Tags for the schema (e.g., project=blockchain,env=prod)")
    # File format arguments
    schema_args.add_argument('--create-file-format', type=str, dest='file_format_name', help="File format name")
    schema_args.add_argument('--file-format-type', choices=['CSV', 'JSON', 'PARQUET'], default='CSV', help="File format type")
    schema_args.add_argument('--file-format-field-delimiter', type=str, default='\t', help="Field delimiter for file format")
    schema_args.add_argument('--file-format-field-optionally-enclosed-by', type=str, default=None, help="Field enclosure character (e.g., '\"')")
    schema_args.add_argument('--file-format-skip-header', type=int, default=1, help="Number of header rows to skip")
    schema_args.add_argument('--file-format-comment', type=str, default='File format for TSV files used in blockchain ETL processes', help="Comment for the file format")
    # Stage arguments
    schema_args.add_argument('--create-stage', type=str, dest='stage_name', help="Stage name")
    schema_args.add_argument('--stage-file-format', type=str, default='tsv_file', help="File format for the stage")
    schema_args.add_argument('--stage-comment', type=str, default='Stage for TSV files used in blockchain ETL processes', help="Comment for the stage")
    # Table arguments
    schema_args.add_argument('--create-table', type=str, dest='table_name', help="Table name")
    schema_args.add_argument('--table-sql-path', type=str, help="Path to .sql file containing CREATE TABLE statement")
    schema_args.add_argument('--table-file-path', type=str, help="Path in stage to load data from (e.g., 'bitcoin_transactions')")
    schema_args.add_argument('--table-file-format', type=str, default='tsv_file', help="File format for table data loading")
    schema_args.add_argument('--table-comment', type=str, default='Table for blockchain data', help="Comment for the table")

    args = parser.parse_args()

    logger = setup_logging()
    logger.info(f"Script {SCRIPT_NAME} started. Version: {__version__}")
    logger.info(f"Execution started from Host: {socket.gethostname()}")
    logger.info(f"Connecting to Snowflake account: {snowflake_config['account']} with user: {snowflake_config['user']} and role: {snowflake_config['role']}")

    try:
        with snowflake.connector.connect(**snowflake_config) as conn:
            with conn.cursor(DictCursor) as cursor:
                if args.create_warehouse:
                    create_warehouse(cursor, logger,
                                    create_warehouse=args.create_warehouse,
                                    warehouse_type=args.warehouse_type,
                                    warehouse_size=args.warehouse_size,
                                    comment=args.warehouse_comment)
                if args.database_name and not args.schema_name and not args.file_format_name and not args.stage_name and not args.table_name:
                    create_database(cursor, logger,
                                    database_name=args.database_name,
                                    or_replace=args.database_or_replace,
                                    transient=args.database_transient,
                                    comment=args.database_comment,
                                    tags=args.database_tags)
                if args.schema_name:
                    if not args.database_name:
                        raise ValueError("schema-database-name is required for schema creation")
                    create_schema(cursor, logger,
                                  database_name=args.database_name,
                                  schema_name=args.schema_name,
                                  or_replace=args.schema_or_replace,
                                  transient=args.schema_transient,
                                  comment=args.schema_comment,
                                  tags=args.schema_tags)
                if args.file_format_name:
                    if not args.database_name or not args.schema_name:
                        raise ValueError("schema-database-name and create-schema are required for file format creation")
                    create_file_format(cursor, logger,
                                      database_name=args.database_name,
                                      schema_name=args.schema_name,
                                      file_format_name=args.file_format_name,
                                      type=args.file_format_type,
                                      field_delimiter=args.file_format_field_delimiter,
                                      field_optionally_enclosed_by=args.file_format_field_optionally_enclosed_by,
                                      skip_header=args.file_format_skip_header,
                                      comment=args.file_format_comment)
                if args.stage_name:
                    if not args.database_name or not args.schema_name:
                        raise ValueError("schema-database-name and create-schema are required for stage creation")
                    create_stage(cursor, logger,
                                 database_name=args.database_name,
                                 schema_name=args.schema_name,
                                 stage_name=args.stage_name,
                                 file_format=args.stage_file_format,
                                 comment=args.stage_comment)
                if args.table_name:
                    if not args.database_name or not args.schema_name:
                        raise ValueError("schema-database-name and create-schema are required for table creation")
                    if not args.table_sql_path:
                        raise ValueError("table-sql-path is required for table creation")
                    create_table(cursor, logger,
                                 database_name=args.database_name,
                                 schema_name=args.schema_name,
                                 table_name=args.table_name,
                                 table_sql_path=args.table_sql_path,
                                 file_path=args.table_file_path,
                                 file_format=args.table_file_format,
                                 comment=args.table_comment)

    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(f"Snowflake error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

    logger.info(f"Script {SCRIPT_NAME} finished execution.")

if __name__ == "__main__":
    main()