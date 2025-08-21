-- Staged outputs (satoshis)
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
  is_spendable
from {{ source('bitcoin_raw', 'outputs_raw') }}
where transaction_hash is not null