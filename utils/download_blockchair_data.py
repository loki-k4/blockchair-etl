import sys
import re
import logging
import json
import argparse
from typing import List
from pathlib import Path
from datetime import datetime, timedelta
import socket
import requests
from logging.handlers import RotatingFileHandler

# === VERSION ===
SCRIPT_VERSION = "1.0.6"

# === LOGGER SETUP ===
def setup_logging(log_dir: Path, log_level: str = "INFO", max_log_size: int = 10*1024*1024, backup_count: int = 5) -> None:
    """Configure logging with rotation to a single daily file.
    
    Args:
        log_dir: Directory for log files (e.g., <project_root>/logs/downloader/).
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        max_log_size: Maximum log file size in bytes (default: 10MB).
        backup_count: Number of backup log files to keep.
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
    
    # Console handler
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

# === FUNCTION: Download data ===
def download_blockchair_data(coin: str, num_days: int, data_types: List[str], config: DownloadConfig, project_root: Path) -> None:
    """Download Blockchair data to a single directory for specified coin, days, and data types.
    
    Args:
        coin: The cryptocurrency (e.g., bitcoin).
        num_days: Number of days to download (including yesterday).
        data_types: List of data types (e.g., blocks, transactions).
        config: Download configuration object.
        project_root: Path to the project root directory.
    
    Raises:
        ValueError: If inputs are invalid.
        IOError: If download or file operations fail.
    """
    # Validate inputs
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", coin):
        logging.error(f"Invalid coin name: {coin}")
        raise ValueError(f"Invalid coin name: {coin}")
    if num_days <= 0:
        logging.error(f"Number of days must be positive: {num_days}")
        raise ValueError(f"Number of days must be positive: {num_days}")
    if not data_types:
        logging.error("No data types specified")
        raise ValueError("No data types specified")
    for dtype in data_types:
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", dtype):
            logging.error(f"Invalid data type: {dtype}")
            raise ValueError(f"Invalid data type: {dtype}")

    # Create single data directory relative to project root
    dest_dir = project_root / config.base_dir / coin
    dest_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Created directory: {dest_dir}")

    # Download loop: includes yesterday and previous (num_days - 1) days
    for i in range(num_days):
        date_str = (datetime.now() - timedelta(days=i + 1)).strftime("%Y%m%d")
        for dtype in data_types:
            filename = f"blockchair_{coin}_{dtype}_{date_str}.tsv.gz"
            url = f"{config.base_url}/{coin}/{dtype}/{filename}"
            dest = dest_dir / filename

            logging.info(f"Downloading: {url}")
            print(f"🔽 Downloading: {url}")
            try:
                response = requests.get(url, stream=True)
                if response.status_code == 200:
                    with open(dest, 'wb') as f:
                        f.write(response.content)
                    if dest.exists() and dest.stat().st_size > 0:
                        logging.info(f"Saved: {dest}")
                        print(f"✅ Saved: {dest}")
                    else:
                        logging.warning(f"Empty file downloaded: {filename}")
                        print(f"❌ Empty file: {filename}")
                        dest.unlink(missing_ok=True)
                else:
                    logging.warning(f"Failed to download: {url} (Status: {response.status_code})")
                    print(f"❌ Failed: {filename}")
            except Exception as e:
                logging.error(f"Failed to download {url}: {e}")
                print(f"❌ Failed: {filename}")
                dest.unlink(missing_ok=True)

    logging.info(f"Completed downloading {num_days} days of '{coin}' data for {data_types}")
    print(f"✅ Done downloading {num_days} days of '{coin}' data.")

# === MAIN ===
def main():
    """Main function to download Blockchair data."""
    parser = argparse.ArgumentParser(description=f"Download Blockchair data as TSV.GZ files (Version: {SCRIPT_VERSION})")
    parser.add_argument("coin", help="Cryptocurrency (e.g., bitcoin)")
    parser.add_argument("num_days", type=int, help="Number of days to download (including yesterday)")
    parser.add_argument("data_types", nargs='+', help="Data types to download (e.g., blocks transactions outputs)")
    parser.add_argument("--config", default=str(Path(__file__).parent / "config" / "download_config.json"), help="Path to configuration file")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent.parent / "logs" / "downloader"), help="Directory for log files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level")

    args = parser.parse_args()

    # Resolve project root dynamically
    project_root = Path(__file__).parent.parent
    config_path = Path(args.config)
    log_dir = Path(args.log_dir)

    # Validate inputs
    if not config_path.exists():
        logging.error(f"Configuration file not found: {config_path}")
        print(f"[ERROR] Configuration file not found: {config_path}")
        sys.exit(1)

    # Setup logging
    setup_logging(log_dir, args.log_level)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = DownloadConfig(config_path)
        
        # Download data
        download_blockchair_data(args.coin, args.num_days, args.data_types, config, project_root)
    except Exception as e:
        logger.exception(f"Failed to download data: {e}")
        print(f"[ERROR] Failed to download data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()