-- Staged inputs (satoshis)
{{
  config(
    materialized='view',
    cluster_by=['time']
  )
}}

select
  block_id,
  transaction_hash,
  index,
  time,
  value as value_sats,  -- Unit: satoshis
  {{ convert_to_btc('value') }} as value_btc,  -- Unit: BTC
  value_usd as value_usd,  -- Unit: USD
  recipient,
  type,
  script_hex,
  is_from_coinbase,
  is_spendable,
  spending_block_id,
  spending_transaction_hash,
  spending_index,
  spending_time,
  spending_value_usd as spending_value_usd,  -- Unit: USD
  spending_sequence,
  spending_signature_hex,
  spending_witness,
  lifespan as lifespan_secs,  -- Unit: seconds
  lifespan / 86400.0 as lifespan_days,  -- Unit: days
  cdd as cdd_days,  -- Unit: coin-days
  {{ calculate_cdd('lifespan', 'value') }} as calculated_cdd_days  -- Unit: coin-days (for validation)
from {{ source('bitcoin_raw', 'inputs_raw') }}
where transaction_hash is not null