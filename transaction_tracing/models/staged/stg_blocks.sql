{{ config(materialized='view', alias='STG_BLOCKS') }}

SELECT *
FROM {{ source('bitcoin_raw', 'BLOCKS_RAW') }}
WHERE DATE(TIME) = '{{ var('run_date') }}'
  AND BLOCK_ID IS NOT NULL
  AND BLOCK_ID != ''
  AND BLOCK_ID != '0'
  AND BLOCK_ID != '<NULL>'