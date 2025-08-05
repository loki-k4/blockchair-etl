import sys
import re
import logging
import json
import argparse
import gzip
from typing import List, Tuple, Optional
from pathlib import Path
from datetime import datetime
import socket
import pandas as pd
from logging.handlers import RotatingFileHandler
try:
    from colorama import init, Fore
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False

# === VERSION ===
SCRIPT_VERSION = "1.0.5"

# === LOGGER SETUP ===
def setup_logging(
    log_dir: Path,
    log_level: str = "INFO",
    max_log_size: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    no_console_logs: bool = False,
    logger_name: str = __name__
) -> logging.Logger:
    """Configure and return a logger instance with rotating file handler."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ddl_generator_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()

    formatter = logging.Formatter(
        f"%(asctime)s [%(levelname)s] [Host: {socket.gethostname()}] [Version: {SCRIPT_VERSION}] [generate_snowflake_ddl] %(message)s"
    )

    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if not no_console_logs:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logger.info("Logging initialized.")
    return logger

# === CONFIGURATION CLASS ===
class DDLConfig:
    """Configuration for DDL generation."""
    SCHEMA = {
        "default_string_length": int,
        "varchar_tiers": list,
        "date_formats": list,
        "timestamp_formats": list,
        "usecols": list
    }

    def __init__(self, config_path: Path, logger: logging.Logger):
        """Load and validate configuration from a JSON file."""
        self.logger = logger
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            for key, expected_type in self.SCHEMA.items():
                if key not in config:
                    self.logger.warning(f"Missing configuration key: {key}, using default")
                    config[key] = [] if key in ("date_formats", "timestamp_formats", "usecols") else 16777216
                if not isinstance(config[key], expected_type):
                    raise ValueError(f"Invalid type for {key}: expected {expected_type}, got {type(config[key])}")

            self.default_string_length = config["default_string_length"]
            self.varchar_tiers = config["varchar_tiers"]
            self.date_formats = config.get("date_formats", [])
            self.timestamp_formats = config.get("timestamp_formats", [])
            self.usecols = config.get("usecols", [])
            self.logger.info(f"Loaded configuration from {config_path}: default_string_length={self.default_string_length}, varchar_tiers={self.varchar_tiers}, date_formats={self.date_formats}, timestamp_formats={self.timestamp_formats}, usecols={self.usecols}")
        except Exception as e:
            self.logger.error(f"Failed to load configuration from {config_path}: {e}")
            raise

# === FUNCTION: Get next VARCHAR tier ===
def get_varchar_length(max_length: Optional[int], tiers: List[int], default: int) -> int:
    """Determine the appropriate VARCHAR length based on tiers."""
    if max_length is None:
        return default
    for tier in tiers:
        if max_length <= tier:
            return tier
    return tiers[-1]

# === FUNCTION: Detect date/time ===
def is_date_or_timestamp(series: pd.Series, formats: List[str]) -> Optional[str]:
    """Check if a series matches date or timestamp formats."""
    try:
        sample = series.dropna().astype(str).head(100)
        if sample.empty:
            return None
        for fmt in formats:
            try:
                pd.to_datetime(sample, format=fmt, errors='raise')
                return 'DATE' if 'H' not in fmt and 'M' not in fmt else 'TIMESTAMP'
            except ValueError:
                continue
        return None
    except Exception:
        return None

# === FUNCTION: Parse existing schema ===
def parse_existing_schema(ddl_path: Optional[Path], json_path: Optional[Path], logger: logging.Logger) -> List[Tuple[str, str]]:
    """Parse schema from existing DDL or JSON file."""
    schema = []
    if json_path and json_path.exists():
        try:
            with json_path.open('r') as f:
                schema_data = json.load(f)
            schema = [(col['name'].upper(), col['type'].upper()) for col in schema_data]
            logger.info(f"Parsed schema from JSON: {json_path} with {len(schema)} columns")
            return schema
        except Exception as e:
            logger.error(f"Failed to parse schema from JSON {json_path}: {e}")
            raise ValueError(f"Invalid JSON schema file: {e}")
    elif ddl_path and ddl_path.exists():
        try:
            with ddl_path.open('r') as f:
                ddl = f.read()
            # Remove comments and normalize whitespace
            ddl = re.sub(r'--.*?\n|/\*.*?\*/', '', ddl, flags=re.DOTALL)
            # Match column definitions, handling whitespace, commas, and optional lengths
            columns = re.findall(r'(\w+)\s+(\w+(?:\s*\(\s*\d+\s*\))?)\s*(?:,|\))', ddl, re.MULTILINE | re.IGNORECASE)
            schema = [(name.upper(), typ.strip().upper()) for name, typ in columns]
            logger.info(f"Parsed schema from DDL: {ddl_path} with {len(schema)} columns")
            return schema
        except Exception as e:
            logger.error(f"Failed to parse schema from DDL {ddl_path}: {e}")
            raise ValueError(f"Invalid DDL file: {e}")
    logger.debug(f"No existing schema found for DDL: {ddl_path}, JSON: {json_path}")
    return schema

# === FUNCTION: Compare schemas ===
def compare_schemas(new_schema: List[Tuple[str, str]], old_schema: List[Tuple[str, str]], logger: logging.Logger) -> bool:
    """Compare two schemas and determine if new_schema has strictly larger data types."""
    if not old_schema:
        logger.info("No existing schema to compare, using new schema")
        return True

    old_dict = {col: typ for col, typ in old_schema}
    new_dict = {col: typ for col, typ in new_schema}
    common_cols = set(old_dict.keys()) & set(new_dict.keys())

    if not common_cols:
        logger.info("No common columns between schemas, using new schema")
        return True

    has_larger_type = False
    all_equal = True

    for col in common_cols:
        old_type = old_dict[col]
        new_type = new_dict[col]
        logger.debug(f"Comparing column {col}: old={old_type}, new={new_type}")

        # Handle VARCHAR
        if 'VARCHAR' in old_type and 'VARCHAR' in new_type:
            old_len_match = re.search(r'\((\d+)\)', old_type)
            new_len_match = re.search(r'\((\d+)\)', new_type)
            old_len = int(old_len_match.group(1)) if old_len_match else 16777216
            new_len = int(new_len_match.group(1)) if new_len_match else 16777216
            if new_len < old_len:
                logger.info(f"Keeping old schema: VARCHAR length for {col} ({new_len}) is smaller than existing ({old_len})")
                return False
            elif new_len > old_len:
                has_larger_type = True
                all_equal = False
            # If equal, continue checking other columns
        # Handle numeric types
        elif old_type == 'INTEGER' and new_type == 'FLOAT':
            has_larger_type = True
            all_equal = False
        elif old_type == 'FLOAT' and new_type == 'INTEGER':
            logger.info(f"Keeping old schema: INTEGER for {col} is narrower than existing FLOAT")
            return False
        # Handle date/time types
        elif old_type == 'DATE' and new_type == 'TIMESTAMP':
            has_larger_type = True
            all_equal = False
        elif old_type == 'TIMESTAMP' and new_type == 'DATE':
            logger.info(f"Keeping old schema: DATE for {col} is less precise than existing TIMESTAMP")
            return False
        # Handle mismatched types
        elif old_type != new_type:
            logger.info(f"Keeping old schema: Type mismatch for {col} (old={old_type}, new={new_type})")
            return False
        # If types are equal, continue checking
        else:
            continue

    if all_equal and not has_larger_type:
        logger.info("Keeping old schema: All column types are equal")
        return False

    logger.info("New schema has strictly larger types, will replace existing")
    return True

# === FUNCTION: Infer schema ===
def infer_schema(file_path: Path, sample_rows: int, chunk_size: int, config: DDLConfig, logger: logging.Logger) -> List[Tuple[str, str]]:
    """Infer Snowflake schema from a TSV or TSV.GZ file."""
    logger.info(f"Inferring schema from {file_path} using {sample_rows} sample rows, chunk_size={chunk_size}")
    try:
        schema = []
        usecols = config.usecols if config.usecols else None
        rows_read = 0
        chunks = []

        reader = pd.read_csv(
            file_path,
            sep='\t',
            compression='gzip' if file_path.suffix == '.gz' else None,
            nrows=sample_rows,
            chunksize=chunk_size,
            usecols=usecols,
            encoding='utf-8'
        )

        for chunk in reader:
            chunks.append(chunk)
            rows_read += len(chunk)
            if rows_read >= sample_rows:
                break

        df = pd.concat(chunks, ignore_index=True)[:sample_rows]

        for col in df.columns:
            col_name = re.sub(r'[^a-zA-Z0-9]', '_', col).upper().strip('_')
            if not col_name or not col_name[0].isalpha():
                col_name = f"COL_{col_name}" if col_name else f"COL_{df.columns.get_loc(col)}"

            date_type = is_date_or_timestamp(df[col], config.date_formats + config.timestamp_formats)
            if date_type:
                col_type = date_type
            elif df[col].isna().all():
                logger.warning(f"Column {col_name} is all null, defaulting to VARCHAR({config.default_string_length})")
                col_type = f"VARCHAR({config.default_string_length})"
            elif pd.api.types.is_integer_dtype(df[col].dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(df[col].dtype):
                col_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(df[col].dtype):
                col_type = "BOOLEAN"
            else:
                max_length = df[col].dropna().astype(str).str.len().max()
                varchar_length = get_varchar_length(max_length, config.varchar_tiers, config.default_string_length)
                col_type = f"VARCHAR({varchar_length})"

            schema.append((col_name, col_type))
            logger.debug(f"Inferred column: {col_name} => {col_type}")

        if not schema:
            raise ValueError("No columns inferred from the file")
        logger.info(f"Inferred schema with {len(schema)} columns")
        return schema
    except Exception as e:
        logger.exception(f"Failed to infer schema from {file_path}: {e}")
        raise ValueError(f"Failed to infer schema: {e}")

# === FUNCTION: Generate DDL ===
def generate_ddl(table_name: str, schema: List[Tuple[str, str]], logger: logging.Logger) -> str:
    """Generate Snowflake DDL for a table."""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", table_name):
        logger.error(f"Invalid table name: {table_name}")
        raise ValueError(f"Invalid table name: {table_name}")
    if not schema:
        logger.error("Empty schema - cannot generate DDL")
        raise ValueError("Schema is empty")

    columns = [f"{col} {typ}" for col, typ in schema]
    ddl = f"CREATE OR REPLACE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n);"
    logger.info(f"Generated DDL for table `{table_name}` with {len(schema)} columns")
    return ddl

# === MAIN ===
def main():
    """Generate Snowflake DDL from TSV or TSV.GZ files."""
    if COLORAMA_AVAILABLE:
        init()

    parser = argparse.ArgumentParser(
        description=f"Generate Snowflake DDL from TSV or TSV.GZ files (Version: {SCRIPT_VERSION})",
        epilog="""
Examples:
  python generate_snowflake_ddl.py data.tsv blocks_raw --output-ddl sql/ddl/create_blocks.sql --skip-existing
  python generate_snowflake_ddl.py file.tsv.gz inputs_raw --output-ddl sql/ddl/create_inputs.sql
"""
    )
    parser.add_argument("file_path", help="Path to input TSV or TSV.GZ file")
    parser.add_argument("table_name", help="Name of the table for DDL")
    parser.add_argument("--sample-rows", type=int, default=1000, help="Number of rows to sample")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Number of rows per chunk for processing")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config" / "ddl_config.json"), help="Path to configuration file")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent.parent / "logs" / "ddl_generator"), help="Directory for logs")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Log level")
    parser.add_argument("--output-ddl", help="Path to save DDL")
    parser.add_argument("--output-schema-json", help="Path to output inferred schema in JSON")
    parser.add_argument("--no-console-logs", action="store_true", help="Disable console logging")
    parser.add_argument("--skip-existing", action="store_true", help="Compare and keep schema with larger or equal types if output files exist")

    args = parser.parse_args()

    file_path = Path(args.file_path)
    config_path = Path(args.config)
    log_dir = Path(args.log_dir)
    ddl_path = Path(args.output_ddl) if args.output_ddl else None
    json_path = Path(args.output_schema_json) if args.output_schema_json else None

    # Early validation
    if not file_path.exists():
        print(f"{Fore.RED if COLORAMA_AVAILABLE else ''}[ERROR] File not found: {file_path}{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        sys.exit(2)
    if args.sample_rows <= 0:
        print(f"{Fore.RED if COLORAMA_AVAILABLE else ''}[ERROR] Sample rows must be > 0{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        sys.exit(3)
    if args.chunk_size <= 0:
        print(f"{Fore.RED if COLORAMA_AVAILABLE else ''}[ERROR] Chunk size must be > 0{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        sys.exit(4)
    if not config_path.exists():
        print(f"{Fore.RED if COLORAMA_AVAILABLE else ''}[ERROR] Config file not found: {config_path}{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        sys.exit(5)

    logger = setup_logging(log_dir, args.log_level, no_console_logs=args.no_console_logs, logger_name="ddl_generator")

    try:
        config = DDLConfig(config_path, logger)
        new_schema = infer_schema(file_path, args.sample_rows, args.chunk_size, config, logger)

        if args.skip_existing and (ddl_path or json_path):
            old_schema = parse_existing_schema(ddl_path, json_path, logger)
            if old_schema and not compare_schemas(new_schema, old_schema, logger):
                print(f"{Fore.YELLOW if COLORAMA_AVAILABLE else ''}[WARNING] Keeping existing schema with larger or equal types{Fore.RESET if COLORAMA_AVAILABLE else ''}")
                logger.info("Skipped DDL generation due to larger or equal existing schema")
                sys.exit(1)  # Exit with code 1 to indicate skip

        ddl = generate_ddl(args.table_name, new_schema, logger)

        if args.output_ddl:
            output_path = Path(args.output_ddl)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(ddl)
            logger.info(f"Saved DDL to {output_path}")
            print(f"{Fore.GREEN if COLORAMA_AVAILABLE else ''}✅ Saved DDL to {output_path}{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        else:
            print(ddl)
            logger.info("DDL printed to stdout")

        if args.output_schema_json:
            schema_json_path = Path(args.output_schema_json)
            schema_json_path.parent.mkdir(parents=True, exist_ok=True)
            schema_dict = [{"name": col, "type": typ} for col, typ in new_schema]
            schema_json_path.write_text(json.dumps(schema_dict, indent=2))
            logger.info(f"Saved inferred schema JSON to {schema_json_path}")
            print(f"{Fore.GREEN if COLORAMA_AVAILABLE else ''}✅ Saved schema JSON to {schema_json_path}{Fore.RESET if COLORAMA_AVAILABLE else ''}")

        logger.info("DDL generation completed successfully")
        print(f"{Fore.GREEN if COLORAMA_AVAILABLE else ''}✅ DDL generation completed{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        sys.exit(0)  # Success
    except Exception as e:
        logger.exception(f"DDL generation failed: {e}")
        print(f"{Fore.RED if COLORAMA_AVAILABLE else ''}[ERROR] DDL generation failed: {e}{Fore.RESET if COLORAMA_AVAILABLE else ''}")
        sys.exit(5)  # Error
if __name__ == "__main__":
    main()
else:           
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.warning("This script is intended to be run as a standalone program, not imported as a module.")  