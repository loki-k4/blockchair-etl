#!/bin/bash

   # Version: 1.0.2
   # Purpose: Run download_blockchair_data.py in venv daily at 12:30 AM for Bitcoin data, skip existing files, and retain last 2 days
   # Usage: Called by cron, no manual execution required
   # Example cron entry: 30 0 * * * /bin/bash /root/blockchair-etl/scripts/daily_download_blockchair.sh

   SCRIPT_VERSION="1.0.2"
   PROJECT_ROOT="$(realpath "$(dirname "$0")/..")"
   LOG_DIR="${PROJECT_ROOT}/logs/downloader"
   LOG_FILE="${LOG_DIR}/downloader_$(date +%Y%m%d).log"
   DATA_DIR="${PROJECT_ROOT}/crypto-data/bitcoin"
   VENV_ACTIVATE="${PROJECT_ROOT}/.venv/bin/activate"
   HOSTNAME=$(hostname)

   # Log message
   log_message() {
       local level=$1
       local message=$2
       echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] [Host: $HOSTNAME] [Version: $SCRIPT_VERSION] [daily_download_blockchair] $message" >> "$LOG_FILE"
   }

   # Setup logging directory
   mkdir -p "$LOG_DIR"
   if [[ $? -ne 0 ]]; then
       echo "$(date '+%Y-%m-%d %H:%M:%S') [ERROR] [Host: $HOSTNAME] [Version: $SCRIPT_VERSION] [daily_download_blockchair] Failed to create log directory: $LOG_DIR" >> "$LOG_FILE"
       exit 1
   fi

   # Check if virtual environment exists
   if [[ ! -f "$VENV_ACTIVATE" ]]; then
       log_message "ERROR" "Virtual environment not found at $VENV_ACTIVATE"
       exit 1
   fi

   log_message "INFO" "Starting daily download of Bitcoin data"

   # Activate virtual environment and run download_blockchair_data.py
   source "$VENV_ACTIVATE"
   python3 "${PROJECT_ROOT}/utils/download_blockchair_data.py" bitcoin 1 blocks transactions outputs inputs \
       --config "${PROJECT_ROOT}/utils/config/download_config.json" \
       --log-dir "$LOG_DIR" \
       --log-level INFO \
       --skip-existing

   if [[ $? -eq 0 ]]; then
       log_message "INFO" "Completed download of Bitcoin data for $(date -d 'yesterday' +%Y%m%d)"
   else
       log_message "ERROR" "Failed to download Bitcoin data for $(date -d 'yesterday' +%Y%m%d)"
       exit 1
   fi
   deactivate

   # Delete files older than 2 days
   log_message "INFO" "Deleting Bitcoin data older than 2 days from $DATA_DIR"
   find "$DATA_DIR" -type f -name "*.tsv.gz" -mtime +2 -delete
   if [[ $? -eq 0 ]]; then
       log_message "INFO" "Successfully deleted files older than 2 days"
   else
       log_message "WARNING" "Failed to delete some or all files older than 2 days"
   fi