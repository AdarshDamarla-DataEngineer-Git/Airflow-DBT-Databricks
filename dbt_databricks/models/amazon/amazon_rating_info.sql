SELECT
    product_id,
    rating,
    rating_count
FROM
    {{ ref('amazon_staging') }}
GROUP BY
    product_id,
    rating,
    rating_count
HAVING
    COUNT(*) = 1