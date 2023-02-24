-- link to query on Google Cloud 
-- https://console.cloud.google.com/bigquery?sq=347987256202:ec68a2cd71e84b5e85aa429a80554882 (public)

/*
The query is broken down into several Common Table Expressions (CTEs) 
which are used to create intermediate tables to help compute the final result
*/

WITH city_metrics as (
  SELECT
    city,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT user_id) AS users,
    SUM(amount) AS total_amount,
    SUM(amount)/ COUNT(DISTINCT order_id) as efood_basket,
    COUNT(DISTINCT order_id) / COUNT(DISTINCT user_id) as efood_freq,

  FROM
    `efood2022-378812.main_assessment.orders`
  GROUP BY
    city
  HAVING
    COUNT(DISTINCT order_id) > 1000
 ), 

 city_metrics_breakfast as (
  SELECT
    city,
    COUNT(DISTINCT order_id) AS breakfast_orders,
    COUNT(DISTINCT user_id) AS breakfast_users,
    SUM(amount) AS breakfast_total_amount,
    SUM(amount)/ COUNT(DISTINCT order_id) as breakfast_basket,
    COUNT(DISTINCT order_id) / COUNT(DISTINCT user_id) as breakfast_freq,
    

  FROM
    `efood2022-378812.main_assessment.orders`
  
  WHERE cuisine = 'Breakfast'
  GROUP BY city
  HAVING
    COUNT(DISTINCT order_id) > 1000

    ),

  merged_city_metrics as (
    SELECT *

    FROM city_metrics 
    LEFT JOIN  city_metrics_breakfast USING (city)
  ),

efood_user_metrics_per_city_with_freq_over_3 as (
  SELECT
    city,
    user_id,
    COUNT(user_id) as efood_user_freq,
    SUM(amount) as efood_user_amount,
    SUM(amount)/ COUNT(user_id) as efood_user_basket
  FROM
    `efood2022-378812.main_assessment.orders`
  
  GROUP BY city, user_id
  HAVING efood_user_freq > 3

), 

breakfast_user_metrics_per_city_with_freq_over_3 as (
  SELECT
    city,
    user_id,
    COUNT(user_id) as breakfast_user_freq,
    SUM(amount) as breakfast_user_amount,
    SUM(amount)/ COUNT(user_id) as breakfast_user_basket
  FROM
    `efood2022-378812.main_assessment.orders`
  
  WHERE cuisine = 'Breakfast'
  GROUP BY city, user_id
  HAVING breakfast_user_freq > 3

), 

frequent_efood_users_count_per_city as (
  SELECT 
  city, 
  COUNT(DISTINCT user_id) as frequent_efood_users
  FROM efood_user_metrics_per_city_with_freq_over_3
  GROUP BY city
),

frequent_breakfast_users_count_per_city as (
  SELECT 
  city, 
  COUNT(DISTINCT user_id) as frequent_breakfast_users
  FROM breakfast_user_metrics_per_city_with_freq_over_3
  GROUP BY city
),

merged_frequent_users_per_city as (
  SELECT *
  FROM frequent_efood_users_count_per_city 
  LEFT JOIN frequent_breakfast_users_count_per_city USING (city)
)
SELECT *,
  frequent_efood_users/ users as efood_users3_freq_prc,
  frequent_breakfast_users/ breakfast_users as breakfast_users3_freq_prc,
FROM merged_city_metrics 
LEFT JOIN merged_frequent_users_per_city USING (city)
ORDER BY merged_city_metrics.breakfast_orders DESC
LIMIT 5


