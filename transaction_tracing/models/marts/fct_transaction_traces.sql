{{
  config(
    materialized='table',
    cluster_by=['source_address', 'destination_address', 'tx_time']
  )
}}

select
  input_address as source_address,
  output_address as destination_address,
  transaction_hash,
  block_id,
  tx_time,
  input_value_sats as transferred_value_sats,
  input_value_btc as transferred_value_btc,
  input_value_usd as transferred_value_usd,
  fee_sats,
  fee_btc,
  fee_usd,
  tx_type,
  input_cdd_days,
  block_cdd_days,
  block_reward_btc,
  row_number() over (partition by transaction_hash order by tx_time) as trace_sequence
from {{ ref('int_transaction_flows') }}