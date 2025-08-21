-- Staged blocks (satoshis)
{{
  config(
    materialized='view',
    cluster_by=['time']
  )
}}

select
  id as block_id,
  hash as block_hash,
  time as block_time,
  median_time as median_block_time,
  size as size_bytes,  -- Unit: bytes
  stripped_size as stripped_size_bytes,  -- Unit: bytes
  weight as weight_wu,  -- Unit: weight units (WU)
  version,
  version_hex,
  version_bits,
  merkle_root,
  nonce,
  bits,
  difficulty,
  chainwork,
  coinbase_data_hex,
  transaction_count,
  witness_count,
  input_count,
  output_count,
  input_total as input_total_sats,  -- Unit: satoshis
  {{ convert_to_btc('input_total') }} as input_total_btc,  -- Unit: BTC
  input_total_usd as input_total_usd,  -- Unit: USD
  output_total as output_total_sats,  -- Unit: satoshis
  {{ convert_to_btc('output_total') }} as output_total_btc,  -- Unit: BTC
  output_total_usd as output_total_usd,  -- Unit: USD
  fee_total as fee_total_sats,  -- Unit: satoshis
  {{ convert_to_btc('fee_total') }} as fee_total_btc,  -- Unit: BTC
  fee_total_usd as fee_total_usd,  -- Unit: USD
  fee_per_kb as fee_per_kb_sats,  -- Unit: satoshis/kB
  fee_per_kb_usd as fee_per_kb_usd,  -- Unit: USD/kB
  fee_per_kwu as fee_per_kwu_sats,  -- Unit: satoshis/kWU
  fee_per_kwu_usd as fee_per_kwu_usd,  -- Unit: USD/kWU
  cdd_total as cdd_total_days,  -- Unit: coin-days
  generation as generation_sats,  -- Unit: satoshis
  {{ convert_to_btc('generation') }} as generation_btc,  -- Unit: BTC
  generation_usd as generation_usd,  -- Unit: USD
  reward as reward_sats,  -- Unit: satoshis
  {{ convert_to_btc('reward') }} as reward_btc,  -- Unit: BTC
  reward_usd as reward_usd,  -- Unit: USD
  guessed_miner
from {{ source('bitcoin_raw', 'blocks_raw') }}
where block_id is not null