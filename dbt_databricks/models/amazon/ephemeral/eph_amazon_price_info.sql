SELECT
    *,
    CAST(TRIM(REPLACE(discount_percentage,"%","")) AS INT) AS discount_percentage_new
FROM
    {{ ref('amazon_price_info') }}