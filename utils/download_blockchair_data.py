import sys
import logging
import requests
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from logging.handlers import RotatingFileHandler
import socket

# === VERSION ===
SCRIPT_VERSION = "1.0.5"

# === LOGGER SETUP ===
def setup_logging(log_dir: Path, log_level: str = "INFO", max_log_size: int = 10*1024*1024, backup_count: int = 5, no_console_log: bool = False) -> None:
    """Configure logging with rotation to a single daily file.

    Args:
        log_dir: Directory for log files (e.g., <project_root>/logs/downloader/).
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        max_log_size: Maximum log file size in bytes (default: 10MB).
        backup_count: Number of backup log files to keep.
        no_console_log: If True, disable console logging.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"downloader_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper()))

    formatter = logging.Formatter(
        f"%(asctime)s [%(levelname)s] [Host: {socket.gethostname()}] [Version: {SCRIPT_VERSION}] [download_blockchair_data] %(message)s"
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_size, backupCount=backup_count)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler (optional)
    if not no_console_log:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logging.info("Logging initialized.")

# === CONFIGURATION CLASS ===
class DownloadConfig:
    """Configuration for Blockchair data download."""
    SCHEMA = {
        "base_url": str,
        "base_dir": str
    }

    def __init__(self, config_path: Path):
        """Load and validate configuration from a JSON file.

        Args:
            config_path: Path to the configuration file.

        Raises:
            ValueError: If configuration is invalid or missing required keys.
        """
        import json
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)

            # Validate configuration
            for key, expected_type in self.SCHEMA.items():
                if key not in config:
                    raise ValueError(f"Missing configuration key: {key}")
                if not isinstance(config[key], expected_type):
                    raise ValueError(f"Invalid type for {key}: expected {expected_type}, got {type(config[key])}")

            self.base_url = config["base_url"]
            self.base_dir = config["base_dir"]
            logging.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logging.error(f"Failed to load configuration from {config_path}: {e}")
            raise

# === FUNCTION: Download file ===
def download_file(url: str, output_path: Path, skip_existing: bool) -> bool:
    """Download a file from a URL to the specified path.

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
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logging.info(f"Saved: {output_path}")
            return True
        else:
            logging.error(f"Failed to download {url}: HTTP {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"Failed to download {url}: {e}")
        return False

# === MAIN ===
def main():
    """Main function to download Blockchair data."""
    parser = argparse.ArgumentParser(description=f"Download Blockchair data files (Version: {SCRIPT_VERSION})")
    parser.add_argument("coin", help="Cryptocurrency (e.g., bitcoin, ethereum)")
    parser.add_argument("num_days", type=int, help="Number of days to download")
    parser.add_argument("data_types", nargs='+', help="Data types to download (e.g., blocks transactions)")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config" / "download_config.json"), help="Path to configuration file")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent.parent / "logs" / "downloader"), help="Directory for log files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging")
    parser.add_argument("--skip-existing", action="store_true", help="Skip downloading if file already exists")

    args = parser.parse_args()

    # Resolve project root dynamically
    project_root = Path(__file__).parent.parent
    config_path = Path(args.config)
    log_dir = Path(args.log_dir)

    # Validate inputs
    if not config_path.exists():
        print(f"[ERROR] Configuration file not found: {config_path}")
        sys.exit(1)
    if args.num_days <= 0:
        print(f"[ERROR] Number of days must be positive: {args.num_days}")
        sys.exit(1)

    # Setup logging
    setup_logging(log_dir, args.log_level, no_console_log=args.no_console_log)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = DownloadConfig(config_path)

        # Generate dates for downloading
        today = datetime.now().date()
        dates = [today - timedelta(days=x) for x in range(1, args.num_days + 1)]

        # Download files for each data type and date
        for data_type in args.data_types:
            for date in dates:
                date_str = date.strftime("%Y%m%d")
                file_name = f"blockchair_{args.coin}_{data_type}_{date_str}.tsv.gz"
                url = f"{config.base_url}/{args.coin}/{data_type}/{file_name}"
                output_path = project_root / config.base_dir / args.coin / file_name

                if not download_file(url, output_path, args.skip_existing):
                    logger.error(f"Download failed for {file_name}")
                    sys.exit(1)

        logger.info("Download completed successfully")
        print("✅ Download completed")
    except Exception as e:
        logger.exception(f"Download failed: {e}")
        print(f"[ERROR] Download failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()