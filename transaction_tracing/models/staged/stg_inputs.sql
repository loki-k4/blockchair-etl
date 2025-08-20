{{
  config(
    materialized='view',
    schema='staged'
  )
}}

-- Values in satoshis, USD as-is
SELECT
  block_id,
  transaction_hash,
  index,
  time,
  value AS value_satoshis,
  value_usd,
  recipient,
  type,
  script_hex,
  is_from_coinbase,
  is_spendable,
  spending_block_id,
  spending_transaction_hash,
  spending_index,
  spending_time,
  spending_value_usd,
  spending_sequence,
  spending_signature_hex,
  spending_witness,
  lifespan,
  cdd
FROM {{ source('bitcoin_raw', 'inputs_raw') }}
WHERE transaction_hash IS NOT NULL
  AND index IS NOT NULL
  AND value IS NOT NULL