{{ config(severity='warn') }}

SELECT 
    *
FROM 
    {{ ref('amazon_staging') }}
WHERE
    product_id IS NULL