SELECT
    product_id,
    discounted_price,
    actual_price,
    discount_percentage
FROM
    {{ ref('amazon_staging') }}
GROUP BY
    product_id,
    discounted_price,
    actual_price,
    discount_percentage
HAVING
    COUNT(*) = 1