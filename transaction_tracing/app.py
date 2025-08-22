#!/usr/bin/env python3
"""
Bitcoin Transaction Tracing Dashboard using Streamlit and Snowflake.
Version: 1.0.1
Logs to logs/streamlit/streamlit_YYYYMMDD.log in JSON format with daily rotation.
Located in blockchair-etl/transaction_tracing/app.py; loads .env from blockchair-etl/config/.env.
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import logging
import json
from logging.handlers import TimedRotatingFileHandler
import time
import argparse
import socket
import getpass
import uuid
from pathlib import Path
import pytz
import re
from tenacity import retry, stop_after_attempt, wait_exponential
import sys

# === VERSION ===
SCRIPT_VERSION = "1.0.1"
SCRIPT_NAME = Path(__file__).name
SESSION_ID = str(uuid.uuid4())
USER = getpass.getuser()

# === EXIT CODES ===
EXIT_SUCCESS = 0
EXIT_INVALID_ARGS = 1
EXIT_CONFIG_ERROR = 2
EXIT_EXECUTION_ERROR = 5

# === LOGGER SETUP ===
class JsonFormatter(logging.Formatter):
    """Custom formatter for JSON-structured logs."""
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(tz=pytz.timezone('America/New_York')).strftime('%Y-%m-%dT%H:%M:%SZ'),
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "session_id": SESSION_ID,
            "user": USER,
            "host": socket.gethostname(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "line": record.lineno
        }
        return json.dumps(log_entry)

def setup_logging(log_dir: Path, log_level: str = "INFO", backup_count: int = 7, no_console_log: bool = False) -> None:
    """Configure logging with daily rotation to a JSON-formatted file."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"streamlit_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()  # Clear existing handlers

    file_handler = TimedRotatingFileHandler(
        filename=log_file,
        when='midnight',
        interval=1,
        backupCount=backup_count
    )
    file_handler.setFormatter(JsonFormatter())
    file_handler.suffix = "%Y%m%d"
    logger.addHandler(file_handler)

    if not no_console_log:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(JsonFormatter())
        logger.addHandler(console_handler)

    logging.info("Logging initialized.")

# === Parse Command-Line Arguments ===
def parse_args():
    """Parse command-line arguments for logging and environment configuration."""
    parser = argparse.ArgumentParser(description=f"Bitcoin Transaction Tracing Dashboard (Version: {SCRIPT_VERSION})")
    parser.add_argument("--log-dir", default=str(Path(__file__).parent / "logs" / "streamlit"), help="Directory for log files")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging level")
    parser.add_argument("--no-console-log", action="store_true", help="Disable console logging")
    parser.add_argument("--env-path", default=str(Path(__file__).parent.parent / "config" / ".env"), help="Path to .env file")
    return parser.parse_args()

# === Validate Bitcoin Address ===
def is_valid_bitcoin_address(address: str) -> bool:
    """Validate a Bitcoin address format."""
    pattern = r"^(1[1-9A-HJ-NP-Za-km-z]{25,34}|3[1-9A-HJ-NP-Za-km-z]{25,34}|bc1[0-9a-z]{39,59})$"
    return bool(re.match(pattern, address))

# === Main Setup ===
args = parse_args()

# Setup logging
setup_logging(Path(args.log_dir), args.log_level, no_console_log=args.no_console_log)

# Load environment variables
env_path = Path(args.env_path)
if not env_path.exists():
    error_msg = f"Environment file not found: {env_path}"
    logging.error(error_msg)
    st.error(error_msg)
    sys.exit(EXIT_CONFIG_ERROR)

try:
    load_dotenv(env_path)
    logging.info(f"Loaded environment variables from {env_path}")
except Exception as e:
    error_msg = f"Failed to load .env file {env_path}: {str(e)}"
    logging.error(error_msg)
    st.error(error_msg)
    sys.exit(EXIT_CONFIG_ERROR)

required_env_vars = [
    "SNOWSQL_ACCOUNT",
    "SNOWSQL_USER",
    "SNOWSQL_PWD",
    "SNOWSQL_ROLE",
    "SNOWSQL_WAREHOUSE",
    "SNOWSQL_DATABASE",
    "SNOWSQL_SCHEMA"
]
for var in required_env_vars:
    value = os.getenv(var)
    if var == "SNOWSQL_PWD":
        value = "****" if value else None
    logging.debug(f"Environment variable {var}: {'set' if value else 'not set'}")

missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    error_msg = f"Missing environment variables: {', '.join(missing_vars)}. Check {env_path} or set them directly."
    logging.error(error_msg)
    st.error(error_msg)
    sys.exit(EXIT_CONFIG_ERROR)

# Streamlit page configuration
st.set_page_config(
    page_title="Bitcoin Transaction Tracing Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake connection
@st.cache_resource
def get_snowflake_connection():
    logging.info("Establishing Snowflake connection")
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWSQL_ACCOUNT"),
            user=os.getenv("SNOWSQL_USER"),
            password=os.getenv("SNOWSQL_PWD"),
            role=os.getenv("SNOWSQL_ROLE"),
            database=os.getenv("SNOWSQL_DATABASE"),
            warehouse=os.getenv("SNOWSQL_WAREHOUSE"),
            schema="BITCOIN_RAW_ANALYTICS",
            login_timeout=30,
            network_timeout=60
        )
        logging.info("Snowflake connection established")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {str(e)}")
        st.error(f"Failed to connect to database: {str(e)}. Please check credentials in {env_path}.")
        raise e

# Function to run Snowflake query with parameters
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
@st.cache_data(ttl=600, hash_funcs={tuple: lambda x: str(x)})
def run_query(query: str, params: tuple = ()) -> pd.DataFrame:
    logging.info(f"Executing query: {query[:100]}... with params: {params}")
    start_time = time.time()
    conn = get_snowflake_connection()
    cur = None
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        df = cur.fetch_pandas_all()
        logging.info(f"Query completed in {time.time() - start_time:.2f} seconds, returned {len(df)} rows")
        if not df.empty:
            logging.debug(f"Query result columns: {list(df.columns)}")
        else:
            logging.debug("Query returned empty DataFrame")
        return df
    except Exception as e:
        logging.error(f"Query failed: {str(e)}")
        raise e
    finally:
        if cur:
            cur.close()

# Check table existence
def check_tables_exist():
    logging.info("Checking table existence")
    conn = get_snowflake_connection()
    cur = conn.cursor()
    tables = [
        ("BITCOIN_RAW_ANALYTICS.fct_transaction_traces",),
        ("BITCOIN_RAW_ANALYTICS.dim_addresses",),
        ("BITCOIN_RAW_ANALYTICS.dim_blocks",),
        ("BITCOIN_RAW_INTERMEDIATE.int_address_balances_with_history",)
    ]
    for table in tables:
        try:
            cur.execute(f"SELECT 1 FROM {os.getenv('SNOWSQL_DATABASE')}.{table[0]} LIMIT 1")
            logging.info(f"Table {table[0]} exists and is accessible")
        except Exception as e:
            logging.error(f"Table {table[0]} does not exist or is not accessible: {str(e)}")
            st.error(f"Table {table[0]} is missing or not accessible")
            sys.exit(EXIT_EXECUTION_ERROR)
    cur.close()

# App title
st.title("Bitcoin Transaction Tracing Dashboard")
st.markdown("""
Explore Bitcoin transaction flows, address balances, and block metrics.  
**Units**: Values in satoshis (1 BTC = 10^8 satoshis), BTC, USD; sizes in bytes, weight units; time in days.
""")

# Check tables before proceeding
check_tables_exist()

# Sidebar for filters
st.sidebar.header("Filters")
address_filter = st.sidebar.text_input("Starting Address", value="", help="Enter a Bitcoin address to trace transactions")
if st.sidebar.button("Suggest Active Address"):
    active_address_query = """
    SELECT source_address
    FROM BLOCKCHAIR.BITCOIN_RAW_ANALYTICS.fct_transaction_traces
    WHERE tx_time BETWEEN %s AND %s
    GROUP BY source_address
    ORDER BY COUNT(*) DESC
    LIMIT 1
    """
    try:
        active_address_df = run_query(active_address_query, (start_date_str, end_date_str))
        if not active_address_df.empty:
            suggested_address = active_address_df["SOURCE_ADDRESS"].iloc[0]
            st.sidebar.text_input("Starting Address", value=suggested_address, key="suggested_address")
            logging.info(f"Suggested active address: {suggested_address}")
        else:
            st.sidebar.warning("No active addresses found for the selected date range.")
            logging.warning("No active addresses found for suggestion")
    except Exception as e:
        logging.error(f"Failed to fetch suggested address: {str(e)}")
        st.sidebar.error("Failed to fetch suggested address")

max_hops = st.sidebar.slider("Max Hops for Tracing", min_value=1, max_value=3, value=1, help="Number of transaction hops (lower for better performance)")
date_range = st.sidebar.date_input(
    "Transaction Date Range",
    value=(datetime.now() - timedelta(days=3), datetime.now()),
    min_value=datetime(2009, 1, 3),
    max_value=datetime.now(),
    help="Select date range for transactions (narrower range improves performance)"
)

# Convert date range to strings for SQL
start_date, end_date = date_range
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Log user filter inputs
logging.info(f"User set filters: address={address_filter}, max_hops={max_hops}, date_range=({start_date_str}, {end_date_str})")

# Validate Bitcoin address
if address_filter and not is_valid_bitcoin_address(address_filter):
    logging.error(f"Invalid Bitcoin address: {address_filter}")
    st.error("Invalid Bitcoin address format.")
    st.stop()

# Key Metrics
st.header("Key Metrics")
col1, col2, col3 = st.columns(3)

# Total transactions
try:
    tx_count_query = f"""
    SELECT /*+ NO_INDEX */ COUNT(DISTINCT transaction_hash) as TOTAL_TRANSACTIONS
    FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.fct_transaction_traces
    WHERE tx_time BETWEEN %s AND %s
    """
    with st.spinner("Fetching total transactions..."):
        tx_count_df = run_query(tx_count_query, (start_date_str, end_date_str))
    total_tx = tx_count_df["TOTAL_TRANSACTIONS"].iloc[0]
    col1.metric("Total Transactions", f"{total_tx:,}")
except Exception as e:
    logging.error(f"Total transactions query failed: {str(e)}")
    col1.error("Failed to load total transactions")

# Average fee (BTC)
try:
    avg_fee_query = f"""
    SELECT /*+ NO_INDEX */ COALESCE(AVG(NULLIF(fee_btc, 0)), 0) as AVG_FEE_BTC
    FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.fct_transaction_traces
    WHERE tx_time BETWEEN %s AND %s
    """
    with st.spinner("Fetching average fee..."):
        avg_fee_df = run_query(avg_fee_query, (start_date_str, end_date_str))
    avg_fee = avg_fee_df["AVG_FEE_BTC"].iloc[0]
    col2.metric("Average Transaction Fee (BTC)", f"{avg_fee or 0:.8f}")
except Exception as e:
    logging.error(f"Average fee query failed: {str(e)}")
    col2.error("Failed to load average fee")

# Top address by balance
try:
    top_address_query = f"""
    SELECT /*+ NO_INDEX */ address AS ADDRESS, current_balance_btc AS CURRENT_BALANCE_BTC
    FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.dim_addresses
    ORDER BY CURRENT_BALANCE_BTC DESC
    LIMIT 1
    """
    with st.spinner("Fetching top address..."):
        top_address_df = run_query(top_address_query)
    top_address = top_address_df["ADDRESS"].iloc[0]
    top_balance = top_address_df["CURRENT_BALANCE_BTC"].iloc[0]
    col3.metric("Top Address Balance (BTC)", f"{top_balance:.2f}", top_address[:8] + "...")
except Exception as e:
    logging.error(f"Top address query failed: {str(e)}")
    col3.error("Failed to load top address")

# Transaction Tracing
st.header("Transaction Flow Tracing")
if address_filter:
    try:
        trace_query = f"""
        WITH RECURSIVE trace_path AS (
            SELECT /*+ NO_INDEX */
                source_address AS SOURCE_ADDRESS,
                destination_address AS DESTINATION_ADDRESS,
                transaction_hash AS TRANSACTION_HASH,
                block_id AS BLOCK_ID,
                tx_time AS TX_TIME,
                transferred_value_btc AS TRANSFERRED_VALUE_BTC,
                transferred_value_usd AS TRANSFERRED_VALUE_USD,
                1 AS HOP
            FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.fct_transaction_traces
            WHERE source_address = %s
                AND tx_time BETWEEN %s AND %s
            UNION ALL
            SELECT /*+ NO_INDEX */
                t.source_address AS SOURCE_ADDRESS,
                t.destination_address AS DESTINATION_ADDRESS,
                t.transaction_hash AS TRANSACTION_HASH,
                t.block_id AS BLOCK_ID,
                t.tx_time AS TX_TIME,
                t.transferred_value_btc AS TRANSFERRED_VALUE_BTC,
                t.transferred_value_usd AS TRANSFERRED_VALUE_USD,
                p.HOP + 1
            FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.fct_transaction_traces t
            JOIN trace_path p ON p.DESTINATION_ADDRESS = t.SOURCE_ADDRESS
            WHERE p.HOP < %s
                AND t.tx_time BETWEEN %s AND %s
        )
        SELECT
            t.*,
            b.block_time AS BLOCK_TIME,
            b.reward_btc AS REWARD_BTC
        FROM trace_path t
        JOIN {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.dim_blocks b ON t.BLOCK_ID = b.block_id
        ORDER BY t.HOP, t.TX_TIME
        LIMIT 1000
        """
        with st.spinner("Tracing transactions..."):
            try:
                trace_df = run_query(trace_query, (address_filter, start_date_str, end_date_str, max_hops, start_date_str, end_date_str))
            except Exception as e:
                if "100298" in str(e) or "Recursive Join ran out of memory" in str(e):
                    logging.warning(f"Recursive query failed due to memory error. Retrying with max_hops=1: {str(e)}")
                    st.warning("Transaction tracing failed due to memory limits. Reducing to 1 hop. Try a larger warehouse or narrower date range.")
                    trace_df = run_query(trace_query, (address_filter, start_date_str, end_date_str, 1, start_date_str, end_date_str))
                else:
                    raise e
        
        if not trace_df.empty:
            # Normalize column names to uppercase
            trace_df.columns = trace_df.columns.str.upper()
            # Validate required columns
            required_columns = ["SOURCE_ADDRESS", "DESTINATION_ADDRESS", "TRANSFERRED_VALUE_BTC"]
            missing_columns = [col for col in required_columns if col not in trace_df.columns]
            if missing_columns:
                logging.error(f"Missing columns in trace_df: {missing_columns}. Available columns: {list(trace_df.columns)}")
                st.error(f"Transaction data has incorrect schema. Missing columns: {', '.join(missing_columns)}. Please check the database schema.")
            else:
                st.subheader(f"Transaction Flow for Address: {address_filter[:8]}...")
                st.dataframe(
                    trace_df,
                    column_config={
                        "SOURCE_ADDRESS": st.column_config.TextColumn("Source Address", width="medium"),
                        "DESTINATION_ADDRESS": st.column_config.TextColumn("Destination Address", width="medium"),
                        "TRANSACTION_HASH": st.column_config.TextColumn("Tx Hash", width="medium"),
                        "TRANSFERRED_VALUE_BTC": st.column_config.NumberColumn("Value (BTC)", format="%.8f"),
                        "TRANSFERRED_VALUE_USD": st.column_config.NumberColumn("Value (USD)", format="$%.2f"),
                        "HOP": st.column_config.NumberColumn("Hop"),
                        "TX_TIME": st.column_config.DatetimeColumn("Tx Time"),
                        "BLOCK_TIME": st.column_config.DatetimeColumn("Block Time"),
                        "REWARD_BTC": st.column_config.NumberColumn("Block Reward (BTC)", format="%.8f")
                    }
                )
                
                # Visualization: Transaction Flow Network
                st.subheader("Transaction Flow Visualization")
                edges = trace_df[["SOURCE_ADDRESS", "DESTINATION_ADDRESS", "TRANSFERRED_VALUE_BTC"]].copy()
                edges["label"] = edges["TRANSFERRED_VALUE_BTC"].apply(lambda x: f"{x:.8f} BTC")
                
                fig = go.Figure(data=[
                    go.Sankey(
                        node=dict(
                            pad=15,
                            thickness=20,
                            line=dict(color="black", width=0.5),
                            label=list(set(edges["SOURCE_ADDRESS"].tolist() + edges["DESTINATION_ADDRESS"].tolist())),
                            color="blue"
                        ),
                        link=dict(
                            source=[list(set(edges["SOURCE_ADDRESS"])).index(src) for src in edges["SOURCE_ADDRESS"]],
                            target=[list(set(edges["DESTINATION_ADDRESS"])).index(dst) for dst in edges["DESTINATION_ADDRESS"]],
                            value=edges["TRANSFERRED_VALUE_BTC"],
                            label=edges["label"]
                        )
                    )
                ])
                fig.update_layout(title_text="Transaction Flow Network", font_size=10)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No transactions found for the given address and filters.")
            logging.warning(f"No transactions found for address: {address_filter}")
    except Exception as e:
        logging.error(f"Transaction tracing failed: {str(e)}")
        st.error("Failed to load transaction flow. Try a smaller date range or contact your Snowflake admin to increase warehouse size.")
        logging.info("Continuing execution despite transaction tracing error")
else:
    st.info("Enter a starting address to trace transactions.")

# Address Balance Trends
st.header("Address Balance Trends")
if address_filter:
    try:
        balance_query = f"""
        SELECT /*+ NO_INDEX */
            time AS TIME,
            running_balance_btc AS RUNNING_BALANCE_BTC,
            running_balance_usd AS RUNNING_BALANCE_USD
        FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_INTERMEDIATE.int_address_balances_with_history
        WHERE address = %s
            AND time BETWEEN %s AND %s
        ORDER BY TIME
        LIMIT 1000
        """
        with st.spinner("Fetching balance trends..."):
            balance_df = run_query(balance_query, (address_filter, start_date_str, end_date_str))
        
        if not balance_df.empty:
            currency = st.selectbox("Currency", ["BTC", "USD"])
            y_column = "RUNNING_BALANCE_BTC" if currency == "BTC" else "RUNNING_BALANCE_USD"
            fig = px.line(
                balance_df,
                x="TIME",
                y=y_column,
                title=f"Balance Trend for Address: {address_filter[:8]}...",
                labels={"TIME": "Date", y_column: f"Balance ({currency})"}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No balance history found for the given address and date range. Verify the address or check the ETL pipeline for recent data.")
            logging.warning(f"No balance history for address: {address_filter}")
    except Exception as e:
        logging.error(f"Balance trends query failed: {str(e)}")
        st.error("Failed to load balance trends. Contact your Snowflake admin to verify table access.")
else:
    st.info("Enter a starting address to view balance trends.")

# Block Metrics
st.header("Block Metrics")
try:
    block_query = f"""
    SELECT /*+ NO_INDEX */
        block_time AS BLOCK_TIME,
        transaction_count AS TRANSACTION_COUNT,
        fee_total_btc AS FEE_TOTAL_BTC,
        reward_btc AS REWARD_BTC,
        cdd_total_days AS CDD_TOTAL_DAYS
    FROM {os.getenv("SNOWSQL_DATABASE")}.BITCOIN_RAW_ANALYTICS.dim_blocks
    WHERE BLOCK_TIME BETWEEN %s AND %s
    ORDER BY BLOCK_TIME
    LIMIT 1000
    """
    with st.spinner("Fetching block metrics..."):
        block_df = run_query(block_query, (start_date_str, end_date_str))

    if not block_df.empty:
        st.dataframe(
            block_df,
            column_config={
                "BLOCK_TIME": st.column_config.DatetimeColumn("Block Time"),
                "TRANSACTION_COUNT": st.column_config.NumberColumn("Transactions"),
                "FEE_TOTAL_BTC": st.column_config.NumberColumn("Total Fees (BTC)", format="%.8f"),
                "REWARD_BTC": st.column_config.NumberColumn("Reward (BTC)", format="%.8f"),
                "CDD_TOTAL_DAYS": st.column_config.NumberColumn("CDD (Coin-Days)")
            }
        )
        
        # Block Fee Trend
        fig = px.line(
            block_df,
            x="BLOCK_TIME",
            y="FEE_TOTAL_BTC",
            title="Block Fee Trend",
            labels={"BLOCK_TIME": "Date", "FEE_TOTAL_BTC": "Total Fees (BTC)"}
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No block data found for the selected date range.")
        logging.warning("No block data found")
except Exception as e:
    logging.error(f"Block metrics query failed: {str(e)}")
    st.error("Failed to load block metrics")

# Footer
st.markdown("---")
st.markdown(f"Built with ❤️ by Blockchair ETL Team | Version {SCRIPT_VERSION}")

# Log session termination
logging.info("Session terminated")