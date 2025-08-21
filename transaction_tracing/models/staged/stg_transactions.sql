-- Staged transactions (satoshis)
{{
  config(
    materialized='view',
    cluster_by=['time']
  )
}}

select
  block_id,
  hash as transaction_hash,
  time as tx_time,
  size as size_bytes,  -- Unit: bytes
  weight as weight_wu,  -- Unit: weight units (WU)
  version,
  lock_time,
  is_coinbase,
  has_witness,
  input_count,
  output_count,
  input_total as input_total_sats,  -- Unit: satoshis
  {{ convert_to_btc('input_total') }} as input_total_btc,  -- Unit: BTC
  input_total_usd as input_total_usd,  -- Unit: USD
  output_total as output_total_sats,  -- Unit: satoshis
  {{ convert_to_btc('output_total') }} as output_total_btc,  -- Unit: BTC
  output_total_usd as output_total_usd,  -- Unit: USD
  fee as fee_sats,  -- Unit: satoshis
  {{ convert_to_btc('fee') }} as fee_btc,  -- Unit: BTC
  fee_usd as fee_usd,  -- Unit: USD
  fee_per_kb as fee_per_kb_sats,  -- Unit: satoshis/kB
  fee_per_kb_usd as fee_per_kb_usd,  -- Unit: USD/kB
  fee_per_kwu as fee_per_kwu_sats,  -- Unit: satoshis/kWU
  fee_per_kwu_usd as fee_per_kwu_usd,  -- Unit: USD/kWU
  cdd_total as cdd_total_days  -- Unit: coin-days
from {{ source('bitcoin_raw', 'transactions_raw') }}
where transaction_hash is not null