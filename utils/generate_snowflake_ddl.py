import sys
import re
import logging
import json
import argparse
import gzip
from typing import List, Tuple
from pathlib import Path
from datetime import datetime
import socket
import pandas as pd
from logging.handlers import RotatingFileHandler

# === VERSION ===
SCRIPT_VERSION = "1.0.4"

# === LOGGER SETUP ===
def setup_logging(
    log_dir: Path,
    log_level: str = "INFO",
    max_log_size: int = 10*1024*1024,
    backup_count: int = 5,
    no_console_logs: bool = False
) -> None:
    """Configure logging with rotation to a single daily file.

    Args:
        log_dir: Directory for log files.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        max_log_size: Maximum log file size in bytes.
        backup_count: Number of backup log files to keep.
        no_console_logs: If True, suppress console logging.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"ddl_generator_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    formatter = logging.Formatter(
        f"%(asctime)s [%(levelname)s] [Host: {socket.gethostname()}] [Version: {SCRIPT_VERSION}] [generate_snowflake_ddl] %(message)s"
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Optional console handler
    if not no_console_logs:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logging.info("Logging initialized.")

# === CONFIGURATION CLASS ===
class DDLConfig:
    """Configuration for DDL generation."""
    SCHEMA = {
        "default_string_length": int,
        "varchar_tiers": list
    }

    def __init__(self, config_path: Path):
        """Load and validate configuration from a JSON file."""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            for key, expected_type in self.SCHEMA.items():
                if key not in config:
                    raise ValueError(f"Missing configuration key: {key}")
                if not isinstance(config[key], expected_type):
                    raise ValueError(f"Invalid type for {key}: expected {expected_type}, got {type(config[key])}")

            self.default_string_length = config["default_string_length"]
            self.varchar_tiers = config["varchar_tiers"]
            logging.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logging.error(f"Failed to load configuration from {config_path}: {e}")
            raise

# === FUNCTION: Get next VARCHAR tier ===
def get_varchar_length(max_length: int, tiers: List[int], default: int) -> int:
    """Return the smallest VARCHAR tier greater than or equal to max_length."""
    if not max_length:
        return default
    for tier in tiers:
        if max_length <= tier:
            return tier
    return tiers[-1]

# === FUNCTION: Infer schema ===
def infer_schema(file_path: Path, sample_rows: int, config: DDLConfig) -> List[Tuple[str, str]]:
    """Infer Snowflake-compatible schema from a TSV or TSV.GZ file."""
    logging.info(f"Inferring schema from {file_path} with {sample_rows} sample rows")
    try:
        if file_path.suffix == '.gz':
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                df = pd.read_csv(f, sep='\t', nrows=sample_rows)
        else:
            df = pd.read_csv(file_path, sep='\t', nrows=sample_rows)

        schema = []
        for col in df.columns:
            col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col).upper()
            if not col_name[0].isalpha():
                col_name = f"COL_{col_name}"

            dtype = df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                col_type = "BOOLEAN"
            else:
                max_length = df[col].dropna().astype(str).str.len().max()
                varchar_length = get_varchar_length(max_length, config.varchar_tiers, config.default_string_length)
                col_type = f"VARCHAR({varchar_length})"

            schema.append((col_name, col_type))
            logging.debug(f"Column {col_name}: {col_type}")

        logging.info(f"Inferred schema with {len(schema)} columns")
        return schema
    except Exception as e:
        logging.error(f"Failed to infer schema from {file_path}: {e}")
        raise ValueError(f"Cannot read file {file_path}: {e}")

# === FUNCTION: Generate DDL ===
def generate_ddl(table_name: str, schema: List[Tuple[str, str]]) -> str:
    """Generate Snowflake DDL for a table."""
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", table_name):
        logging.error(f"Invalid table name: {table_name}")
        raise ValueError(f"Invalid table name: {table_name}")
    if not schema:
        logging.error("Schema is empty")
        raise ValueError("Schema is empty")

    columns = [f"{col_name} {col_type}" for col_name, col_type in schema]
    ddl = f"CREATE OR REPLACE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n);"
    logging.info(f"Generated DDL for table {table_name} with {len(schema)} columns")
    return ddl

# === MAIN ===
def main():
    """Main function to generate Snowflake DDL."""
    parser = argparse.ArgumentParser(description=f"Generate Snowflake DDL from TSV or TSV.GZ files (Version: {SCRIPT_VERSION})")
    parser.add_argument("file_path", help="Path to input TSV or TSV.GZ file")
    parser.add_argument("table_name", help="Name of the table for DDL")
    parser.add_argument("--sample-rows", type=int, default=1000, help="Number of rows to sample for schema inference")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config" / "ddl_config.json"), help="Path to configuration file")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent.parent / "logs" / "ddl_generator"), help="Directory for log files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level")
    parser.add_argument("--output-ddl", default=None, help="Path to save the generated DDL file")
    parser.add_argument("--no-console-logs", action="store_true", help="Disable console logging (logs will only go to file)")

    args = parser.parse_args()

    # Resolve paths
    project_root = Path(__file__).parent.parent
    file_path = Path(args.file_path)
    config_path = Path(args.config)
    log_dir = Path(args.log_dir)

    # Validate inputs
    if not file_path.exists():
        print(f"[ERROR] File not found: {file_path}")
        sys.exit(1)
    if args.sample_rows <= 0:
        print(f"[ERROR] Sample rows must be positive: {args.sample_rows}")
        sys.exit(1)
    if not config_path.exists():
        print(f"[ERROR] Configuration file not found: {config_path}")
        sys.exit(1)

    # Setup logging
    setup_logging(log_dir, args.log_level, no_console_logs=args.no_console_logs)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = DDLConfig(config_path)

        # Infer schema
        schema = infer_schema(file_path, args.sample_rows, config)

        # Generate DDL
        ddl = generate_ddl(args.table_name, schema)

        # Output DDL
        if args.output_ddl:
            output_path = project_root / args.output_ddl
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                f.write(ddl)
            logger.info(f"Saved DDL to {output_path}")
            print(f"✅ Saved DDL to {output_path}")
        else:
            print(ddl)

        logger.info("DDL generation completed successfully")
        print("✅ DDL generation completed")
    except Exception as e:
        logger.exception(f"Failed to generate DDL: {e}")
        print(f"[ERROR] Failed to generate DDL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
