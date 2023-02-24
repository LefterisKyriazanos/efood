-- link to query on Google Cloud 
-- https://console.cloud.google.com/bigquery?sq=347987256202:442f06fc1f384bc9a0c8b3531efd38ca (public)

/*
The query is broken down into several Common Table Expressions (CTEs) 
which are used to create intermediate tables to help compute the final result
*/

WITH city_top_users AS (
  SELECT 
    city, 
    user_id, 
    COUNT(DISTINCT order_id) as user_orders,
  FROM `efood2022-378812.main_assessment.orders`
  GROUP BY city, user_id
  ORDER BY city, user_orders DESC, user_id
),

city_top_user_rankings AS (
  SELECT 
    city, 
    user_id, 
    user_orders, 
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY user_orders DESC) AS user_rank 
  FROM city_top_users
),

orders_of_top_10_users_per_city as (
  SELECT 
    city, 
    SUM(user_orders) AS top_10_users_total_orders
  FROM city_top_user_rankings
  WHERE user_rank <= 10
  GROUP BY city
),

city_total_orders as (
  SELECT
  city,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT user_id) AS users,
  FROM `efood2022-378812.main_assessment.orders`
  GROUP BY city

)

SELECT *,
top_10_users_total_orders/orders as top_10_users_order_contribution, 
FROM city_total_orders
JOIN orders_of_top_10_users_per_city USING (city)
ORDER BY orders DESC

