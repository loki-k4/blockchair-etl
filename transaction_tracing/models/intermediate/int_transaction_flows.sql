{{
  config(
    materialized='table',
    cluster_by=['transaction_hash', 'tx_time']
  )
}}

with blocks as (
  select block_id, block_time, cdd_total_days, reward_btc
  from {{ ref('stg_blocks') }}
),
tx as (
  select * from {{ ref('stg_transactions') }}
),
inputs as (select * from {{ ref('stg_inputs') }}),
outputs as (
  select * from {{ ref('stg_outputs') }}
)

select
  tx.transaction_hash,
  tx.block_id,
  b.block_time as tx_time,
  inputs.recipient as input_address,
  outputs.recipient as output_address,
  inputs.value_sats as input_value_sats,
  inputs.value_btc as input_value_btc,
  outputs.value_sats as output_value_sats,
  outputs.value_btc as output_value_btc,
  inputs.value_usd as input_value_usd,
  outputs.value_usd as output_value_usd,
  tx.fee_sats,
  tx.fee_btc,
  tx.fee_usd,
  inputs.cdd_days as input_cdd_days,
  b.cdd_total_days as block_cdd_days,
  inputs.lifespan_days,
  case
    when inputs.is_from_coinbase = 1 then 'coinbase'
    else 'standard'
  end as tx_type,
  b.reward_btc as block_reward_btc
from tx
left join blocks b on tx.block_id = b.block_id
left join inputs on tx.transaction_hash = inputs.transaction_hash and tx.block_id = inputs.block_id
left join outputs on tx.transaction_hash = outputs.transaction_hash and tx.block_id = outputs.block_id