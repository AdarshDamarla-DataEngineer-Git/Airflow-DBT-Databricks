SELECT 
    product_id,
    discounted_price,
    actual_price,
    discount_percentage
FROM
    {{ ref('eph_amazon_price_info') }}
WHERE
    discount_percentage_new < 10
ORDER BY
    discount_percentage_new DESC