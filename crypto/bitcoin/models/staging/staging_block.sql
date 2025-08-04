-- models/staging/stg_blocks.sql
with raw_blocks as (
    select * from {{ source('bitcoin', 'blocks') }}
)

select
    id as block_id,
    hash,
    time,
    size,
    stripped_size,
    weight,
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
    input_total,
    input_total_usd,
    output_total,
    output_total_usd,
    fee_total,
    fee_total_usd,
    fee_per_kb,
    fee_per_kb_usd,
    fee_per_kwu,
    fee_per_kwu_usd,
    cdd_total,
    generation,
    generation_usd,
    reward,
    reward_usd,
    guessed_miner
from raw_blocks
