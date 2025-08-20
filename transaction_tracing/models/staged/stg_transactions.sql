{{
  config(
    materialized='view',
    schema='staged'
  )
}}

-- Values in satoshis, fees in satoshis/KB or KWU, USD as-is
SELECT
  block_id,
  hash,
  time,
  size,
  weight,
  version,
  lock_time,
  is_coinbase,
  has_witness,
  input_count,
  output_count,
  input_total AS input_total_satoshis,
  input_total_usd,
  output_total AS output_total_satoshis,
  output_total_usd,
  fee AS fee_satoshis,
  fee_usd,
  fee_per_kb AS fee_per_kb_satoshis,
  fee_per_kb_usd,
  fee_per_kwu AS fee_per_kwu_satoshis,
  fee_per_kwu_usd,
  cdd_total
FROM {{ source('bitcoin_raw', 'transactions_raw') }}
WHERE hash IS NOT NULL
  AND block_id IS NOT NULL