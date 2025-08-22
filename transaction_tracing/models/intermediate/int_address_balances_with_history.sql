{{
  config(
    materialized='table',
    cluster_by=['address', 'time']
  )
}}

with addresses as (
  select * from {{ ref('stg_addresses') }}
),
inputs as (
  select
    recipient as address,
    time,
    -value_sats as value_change_sats,  -- Negative for inputs (spent)
    -value_btc as value_change_btc,
    -value_usd as value_change_usd,
    transaction_hash
  from {{ ref('stg_inputs') }}
),
outputs as (
  select
    recipient as address,
    time,
    value_sats as value_change_sats,  -- Positive for outputs (received)
    value_btc as value_change_btc,
    value_usd as value_change_usd,
    transaction_hash
  from {{ ref('stg_outputs') }}
),
changes as (
  select * from inputs
  union all
  select * from outputs
)

select
  a.address,
  c.time,
  c.transaction_hash,
  c.value_change_sats,  -- Include raw value change
  c.value_change_btc,
  c.value_change_usd,
  sum(c.value_change_sats) over (partition by a.address order by c.time) as running_balance_sats,
  sum(c.value_change_btc) over (partition by a.address order by c.time) as running_balance_btc,
  sum(c.value_change_usd) over (partition by a.address order by c.time) as running_balance_usd,
  a.balance_sats as current_balance_sats,
  a.balance_btc as current_balance_btc
from addresses a
left join changes c on a.address = c.address