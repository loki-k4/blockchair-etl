#!/usr/bin/env python3
"""
Downloads Blockchair data for specified cryptocurrencies and data types.
Version: 1.6.0
Downloads only the latest daily file (e.g., 20250813 on 20250814) and retains files for RETENTION_DAYS (default: 3).
Saves files to type-specific subdirectories (e.g., data/bitcoin/blocks/).
Logs to logs/blockchair_etl_YYYYMMDD.log in JSON format for enterprise log management.
Designed for production deployment with robust error handling, logging, and validation.
"""

import sys
import logging
import json
import os
import requests
import argparse
import socket
import getpass
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from jsonschema import validate, ValidationError
import re

# === VERSION ===
SCRIPT_VERSION = "1.6.0"
SCRIPT_NAME = Path(__file__).name
SESSION_ID = str(uuid.uuid4())
USER = getpass.getuser()

# === EXIT CODES ===
EXIT_SUCCESS = 0
EXIT_INVALID_ARGS = 1
EXIT_CONFIG_ERROR = 2
EXIT_EXECUTION_ERROR = 5

# === LOGGER SETUP ===
class JSONFormatter(logging.Formatter):
    """Custom formatter for JSON-structured logs."""
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "session_id": SESSION_ID,
            "user": USER,
            "host": socket.gethostname(),
            "level": record.levelname,
            "message": record.getMessage()
        }
        return json.dumps(log_entry)

def setup_logging(log_dir: Path, log_level: str = "INFO", max_log_size: int = 10*1024*1024, backup_count: int = 5, no_console_log: bool = False) -> None:
    """Configure logging with rotation to a single daily JSON-formatted file.

    Args:
        log_dir: Directory for log files (e.g., logs/).
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        max_log_size: Maximum log file size in bytes (default: 10MB).
        backup_count: Number of backup log files to keep.
        no_console_log: If True, disable console logging.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"blockchair_etl_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()  # Clear existing handlers to avoid duplicates

    # File handler with rotation
    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)

    # Console handler (optional)
    if not no_console_log:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JSONFormatter())
        logger.addHandler(console_handler)

    logging.info("Logging initialized.")

# === CONFIGURATION SCHEMA ===
DOWNLOAD_SCHEMA = {
    "type": "object",
    "properties": {
        "//": {"type": "string"},
        "base_url": {"type": "string", "pattern": "^https?://"},
        "api_key": {"type": ["string", "null"]},
        "data_types": {
            "type": "array",
            "items": {"type": "string", "enum": ["blocks", "transactions", "inputs", "outputs"]}
        },
        "file_format": {"type": "string", "enum": ["tsv.gz"]},
        "destination_dir": {"type": "string"},
        "retention_days": {"type": ["string", "integer"], "pattern": "^[0-9]+$", "minimum": 1},
        "//retention_days": {"type": "string"}
    },
    "required": ["base_url", "data_types", "file_format", "destination_dir", "retention_days"]
}

# === CONFIGURATION CLASS ===
class DownloadConfig:
    """Configuration for Blockchair data download."""
    def __init__(self, config_path: Path):
        """Load and validate configuration from a JSON file.

        Args:
            config_path: Path to the configuration file.

        Raises:
            ValueError: If configuration is invalid or missing required keys.
            ValidationError: If JSON schema validation fails.
        """
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            # Substitute environment variables before validation
            for key, value in config.items():
                if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                    env_var, *default = value[2:-1].split(":-")
                    config[key] = os.environ.get(env_var, default[0] if default else "")
                    if not config[key] and key in DOWNLOAD_SCHEMA["required"]:
                        raise ValueError(f"Environment variable {env_var} not set and no default provided")

            # Convert retention_days to string for validation
            if isinstance(config["retention_days"], int):
                config["retention_days"] = str(config["retention_days"])

            validate(instance=config, schema=DOWNLOAD_SCHEMA)

            self.base_url = config["base_url"]
            self.api_key = config["api_key"]
            self.data_types = config["data_types"]
            self.file_format = config["file_format"]
            self.destination_dir = Path(config["destination_dir"])
            self.retention_days = int(config["retention_days"])
            logging.info(f"Loaded configuration from {config_path}: base_url={self.base_url}, destination_dir={self.destination_dir}, retention_days={self.retention_days}")
        except (ValidationError, ValueError, FileNotFoundError) as e:
            logging.error(f"Failed to load configuration from {config_path}: {e}")
            raise

# === FUNCTION: Load environment variables ===
def load_env(env_path: Path) -> None:
    """Load environment variables from .env file.

    Args:
        env_path: Path to .env file.
    """
    if not env_path.exists():
        logging.warning(f"Environment file not found: {env_path}, proceeding without loading")
        return
    try:
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.replace('export ', '').split('=', 1)
                    os.environ[key] = value.strip('"\'')
        logging.info(f"Loaded environment variables from {env_path}")
    except Exception as e:
        logging.error(f"Failed to load environment variables from {env_path}: {e}")
        raise

# === FUNCTION: Validate Python dependencies ===
def validate_dependencies() -> None:
    """Validate required Python modules are installed."""
    required_modules = ["requests", "jsonschema", "tenacity"]
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            logging.error(f"Required Python module '{module}' is not installed")
            sys.exit(EXIT_EXECUTION_ERROR)

# === FUNCTION: Download file ===
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    reraise=True
)
def download_file(url: str, output_path: Path, skip_existing: bool) -> bool:
    """Download a file from a URL to the specified path with retries.

    Args:
        url: URL of the file to download.
        output_path: Path to save the downloaded file.
        skip_existing: If True, skip download if file already exists.

    Returns:
        True if file was downloaded or skipped (exists), False if download failed.
    """
    if skip_existing and output_path.exists():
        logging.info(f"Skipped existing file: {output_path}")
        return True

    logging.info(f"Downloading: {url}")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        logging.info(f"Saved: {output_path}")
        return True
    except requests.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
        return False

# === FUNCTION: Clean old files ===
def clean_old_files(directory: Path, retention_days: int, today: datetime, data_types: list) -> None:
    """Removes files older than retention_days from type-specific subdirectories.

    Args:
        directory: Base directory (e.g., data/bitcoin/).
        retention_days: Number of days to retain files.
        today: Current date for calculating retention cutoff.
        data_types: List of data types (e.g., ['blocks', 'transactions']).
    """
    valid_dates = {(today - timedelta(days=x)).date().strftime("%Y%m%d") for x in range(1, retention_days + 1)}
    logging.info(f"Retaining files for dates: {valid_dates}")

    for data_type in data_types:
        type_dir = directory / data_type
        for file_path in type_dir.glob("*.tsv.gz"):
            file_name = file_path.name
            # Match blockchair_<coin>_<type>_YYYYMMDD.tsv.gz
            match = re.match(r"blockchair_\w+_(\w+)_(\d{8})\.tsv\.gz", file_name)
            if match:
                file_date_str = match.group(2)
                try:
                    file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                    if file_date_str not in valid_dates:
                        file_path.unlink()
                        logging.info(f"Removed old file: {file_path} (date: {file_date_str})")
                except ValueError:
                    logging.warning(f"Skipping file with invalid date format: {file_path}")
            else:
                logging.warning(f"Skipping file with unexpected name format: {file_path}")

# === MAIN ===
def main():
    """Main function to download Blockchair data."""
    parser = argparse.ArgumentParser(description=f"Download Blockchair data files (Version: {SCRIPT_VERSION})")
    parser.add_argument("coin", nargs='?', default="bitcoin", help="Cryptocurrency (e.g., bitcoin, ethereum)", choices=["bitcoin", "ethereum"])
    parser.add_argument("num_days", nargs='?', type=int, default=1, help="Number of days to download (default: 1 for latest day)")
    parser.add_argument("data_types", nargs='*', default=["blocks", "transactions", "inputs", "outputs"], help="Data types to download (default: blocks transactions inputs outputs)")
    parser.add_argument("--config", default=str(Path(__file__).parent.parent.parent / "config" / "download_config.json"), help="Path to configuration file")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent.parent.parent / "logs"), help="Directory for log files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading if file already exists")

    args = parser.parse_args()

    # Early help check
    if "--help" in sys.argv or "-h" in sys.argv:
        parser.print_help()
        sys.exit(EXIT_SUCCESS)

    # Validate Python dependencies
    validate_dependencies()

    # Resolve project root and paths
    project_root = Path(__file__).parent.parent.parent
    config_path = Path(args.config)
    log_dir = Path(args.log_dir)
    env_path = project_root / "config" / ".env"

    # Validate inputs
    if not config_path.exists():
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(EXIT_CONFIG_ERROR)
    if args.num_days <= 0:
        logging.error(f"Number of days must be positive: {args.num_days}")
        sys.exit(EXIT_INVALID_ARGS)

    # Setup logging
    setup_logging(log_dir, args.log_level, no_console_log=args.no_console_log)

    try:
        # Load environment variables
        load_env(env_path)

        # Load configuration
        config = DownloadConfig(config_path)

        # Validate data types
        invalid_types = set(args.data_types) - set(config.data_types)
        if invalid_types:
            logging.error(f"Invalid data types: {invalid_types}. Supported: {config.data_types}")
            sys.exit(EXIT_INVALID_ARGS)

        # Clean old files based on retention policy
        today = datetime.now()
        clean_old_files(config.destination_dir, config.retention_days, today, config.data_types)

        # Generate date for downloading (yesterday only)
        yesterday = (today - timedelta(days=1)).strftime("%Y%m%d")

        # Download files for each data type
        for data_type in args.data_types:
            file_name = f"blockchair_{args.coin}_{data_type}_{yesterday}.{config.file_format}"
            url = f"{config.base_url}/{args.coin}/{data_type}/{file_name}"
            if config.api_key:
                url += f"?key={config.api_key}"
            output_path = config.destination_dir / data_type / file_name

            if not download_file(url, output_path, args.skip_existing):
                logging.error(f"Download failed for {file_name}")
                sys.exit(EXIT_EXECUTION_ERROR)

        logging.info("Download completed successfully")
    except Exception as e:
        logging.exception(f"Download failed: {e}")
        sys.exit(EXIT_EXECUTION_ERROR)

if __name__ == "__main__":
    main()