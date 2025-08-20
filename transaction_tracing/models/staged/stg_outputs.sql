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
  is_spendable
FROM {{ source('bitcoin_raw', 'outputs_raw') }}
WHERE transaction_hash IS NOT NULL
  AND index IS NOT NULL
  AND value IS NOT NULL