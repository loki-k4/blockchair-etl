{{
  config(
    materialized='view',
    schema='staged'
  )
}}

-- Balance in satoshis
SELECT
  address,
  balance AS balance_satoshis
FROM {{ source('bitcoin_raw', 'address_raw') }}
WHERE address IS NOT NULL