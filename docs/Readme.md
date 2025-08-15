# 🧰 Blockchair ETL Utilities

This directory contains scripts to automate ETL processes for [Blockchair](https://blockchair.com/) cryptocurrency data.

### Tools Included

| Script | Description |
|--------|-------------|
| `download_blockchair_data.py` | 🔽 Download `.tsv.gz` files for selected coins and data types |
| `generate_snowflake_ddl.py`   | 🧱 Infer schema and generate Snowflake `CREATE TABLE` DDL |

---

## 📁 Directory Layout

```bash
utils/
├── config/
│   ├── ddl_config.json           # DDL generation settings
│   └── download_config.json      # Download settings
├── download_blockchair_data.py   # Data downloader
└── generate_snowflake_ddl.py     # Snowflake DDL generator
````

---

## ⚙️ Setup

### 🔧 Prerequisites

* Python 3.8+
* Dependencies: `pandas`, `requests`

### 🧪 Quickstart

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pandas requests
```

---

## ⚙️ Configuration Files

<details>
<summary><strong>📥 download_config.json</strong> (for downloads)</summary>

```json
{
  "base_url": "https://gz.blockchair.com",
  "base_dir": "crypto-data"
}
```

* `base_url`: Base URL for Blockchair data.
* `base_dir`: Directory for downloaded files.

</details>

<details>
<summary><strong>📐 ddl_config.json</strong> (for DDL generation)</summary>

```json
{
  "default_string_length": 64,
  "varchar_tiers": [16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8388608, 16777216]
}
```

* `default_string_length`: Used when no max length is detected.
* `varchar_tiers`: Candidate sizes for `VARCHAR(n)`.

</details>

---

## 📥 Download Data

Script: `download_blockchair_data.py`

### ▶️ Example

```bash
python utils/download_blockchair_data.py bitcoin 3 blocks transactions \
  --config utils/config/download_config.json \
  --log-dir logs/downloader \
  --log-level INFO
```

### 🧾 Arguments

| Name          | Description                                       |
| ------------- | ------------------------------------------------- |
| `coin`        | Coin name (e.g. `bitcoin`, `ethereum`)            |
| `num_days`    | Number of days to download (ending yesterday)     |
| `data_types`  | One or more types: `blocks`, `transactions`, etc. |
| `--config`    | Path to config file                               |
| `--log-dir`   | Directory to write logs                           |
| `--log-level` | `DEBUG`, `INFO`, `WARNING`, `ERROR`               |

---

## 🧱 Generate Snowflake DDL

Script: `generate_snowflake_ddl.py`

### ▶️ Example

```bash
python utils/generate_snowflake_ddl.py crypto-data/bitcoin/blockchair_bitcoin_blocks_20250101.tsv.gz my_blocks_table \
  --sample-rows 1000 \
  --config utils/config/ddl_config.json \
  --log-dir logs/ddl_generator \
  --output-ddl output/my_blocks_table.sql \
  --no-console-logs
```

### 🧾 Arguments

| Name                | Description                               |
| ------------------- | ----------------------------------------- |
| `file_path`         | Path to `.tsv` or `.tsv.gz`               |
| `table_name`        | Target table name                         |
| `--sample-rows`     | Rows to sample for schema (default: 1000) |
| `--config`          | Path to DDL config                        |
| `--log-dir`         | Log output directory                      |
| `--log-level`       | Logging verbosity                         |
| `--output-ddl`      | File path to write generated DDL          |
| `--no-console-logs` | Suppress console output                   |

---

## 📄 Output Example

```sql
CREATE OR REPLACE TABLE my_blocks_table (
    BLOCK_ID INTEGER,
    HASH VARCHAR(64),
    VERSION INTEGER,
    ...
);
```

---

## 📝 Logging

* Logs include timestamps, host, script version, and detailed activity
* Output goes to timestamped files in your `--log-dir`
* Use `--no-console-logs` for silent execution (e.g. in cron jobs)

---

## 🐳 Docker Usage (Optional)

<details>
<summary><strong>🛠️ Basic Dockerfile</strong></summary>

```Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY utils/ ./utils/
RUN pip install pandas requests

CMD ["python", "utils/download_blockchair_data.py"]
```

### Build and Run

```bash
docker build -t blockchair-etl .
docker run --rm blockchair-etl bitcoin 1 blocks
```

</details>

---

## 📃 License

MIT — See main project for full license.
