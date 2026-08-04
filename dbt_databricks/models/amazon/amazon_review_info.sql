SELECT
    product_id,
    user_id,
    user_name,
    review_id,
    review_title,
    review_content
FROM
    {{ ref('amazon_staging') }}
GROUP BY
    product_id,
    user_id,
    user_name,
    review_id,
    review_title,
    review_content
HAVING
    COUNT(*) = 1