{{
  config(
    materialized='table',
    cluster_by=['address']
  )
}}

select
  address,
  current_balance_sats,
  current_balance_btc,
  sum(value_change_sats) as lifetime_value_change_sats,
  sum(value_change_btc) as lifetime_value_change_btc,
  count(distinct transaction_hash) as tx_count
from {{ ref('int_address_balances_with_history') }}
group by 1, 2, 3