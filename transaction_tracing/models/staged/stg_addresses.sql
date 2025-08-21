-- Staged addresses (satoshis)
{{
  config(
    materialized='view',
    cluster_by=['address']
  )
}}

select
  address,
  balance as balance_sats,  -- Unit: satoshis
  {{ convert_to_btc('balance') }} as balance_btc  -- Unit: BTC
from {{ source('bitcoin_raw', 'address_raw') }}
where address is not null