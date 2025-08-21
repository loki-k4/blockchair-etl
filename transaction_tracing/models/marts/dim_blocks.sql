{{
  config(
    materialized='table',
    cluster_by=['block_id']
  )
}}

select
  block_id,
  block_hash,
  block_time,
  difficulty,
  transaction_count,
  fee_total_sats,
  fee_total_btc,
  fee_total_usd,
  reward_sats,
  reward_btc,
  reward_usd,
  cdd_total_days,
  guessed_miner
from {{ ref('stg_blocks') }}