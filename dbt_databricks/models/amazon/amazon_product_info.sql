SELECT
    product_id,
    product_name,
    category,
    about_product,
    img_link,
    product_link
FROM
    {{ ref('amazon_staging') }}
GROUP BY
    product_id,
    product_name,
    category,
    about_product,
    img_link,
    product_link
HAVING
    COUNT(*) = 1