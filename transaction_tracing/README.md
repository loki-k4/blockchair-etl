# Transaction Tracing DBT Project

This DBT project transforms raw blockchain data into analytics-ready models for transaction tracing. It processes data from `ADDRESS_RAW`, `BLOCKS_RAW`, `INPUTS_RAW`, `OUTPUTS_RAW`, and `TRANSACTIONS_RAW` tables, ensuring proper unit handling (satoshis, BTC, USD, bytes, weight units, days).

## Structure
- **staged/**: Cleans raw data, standardizing units (e.g., satoshis, BTC, USD).
- **intermediate/**: Builds derived models for transaction flows and address balance histories.
- **marts/**: Creates fact and dimension tables for tracing funds and analyzing blocks/addresses.
- **macros/**: Reusable functions for unit conversions (e.g., satoshis to BTC, CDD calculations).
- **sources/**: Defines raw data sources.

## Setup
1. Configure `config/profiles.yml` for your Snowflake account.
2. Run `dbt build --profiles-dir config/ --project-dir .` to create models and run tests.
3. Use `dbt docs generate` and `dbt docs serve` for documentation.

## Key Models
- `fct_transaction_traces`: Enables multi-hop fund tracing with source/destination addresses.
- `dim_addresses`: Summarizes address balances and transaction counts.
- `dim_blocks`: Provides block-level metrics (fees, rewards, CDD).

## Example Query
Trace funds from a specific address:
```sql
with recursive trace_path as (
  select
    source_address,
    destination_address,
    transaction_hash,
    block_id,
    tx_time,
    transferred_value_btc,
    transferred_value_usd,
    1 as hop
  from {{ ref('fct_transaction_traces') }}
  where source_address = 'some_address'
  union all
  select
    t.source_address,
    t.destination_address,
    t.transaction_hash,
    t.block_id,
    t.tx_time,
    t.transferred_value_btc,
    t.transferred_value_usd,
    p.hop + 1
  from {{ ref('fct_transaction_traces') }} t
  join trace_path p on p.destination_address = t.source_address
  where p.hop < 5
)
select
  t.*,
  b.block_time,
  b.reward_btc
from trace_path t
join {{ ref('dim_blocks') }} b on t.block_id = b.block_id
order by t.hop, t.tx_time;