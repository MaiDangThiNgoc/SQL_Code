--Task 1: Trending the Data
/* Analyze the trend of payment transactions of Billing category from 2019 to 2020.
First, let’s show the trend of the number of successful transactions by month.
*/

SELECT YEAR(transaction_time) AS year
    , MONTH(transaction_time) AS month
    , CONVERT(varchar(06), transaction_time, 112) AS time_calendar
    , COUNT(transaction_id) AS number_trans 
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND category = 'Billing'
GROUP BY YEAR(transaction_time), MONTH(transaction_time), CONVERT(varchar(06), transaction_time, 112)
ORDER BY year, month 

--Comparing component
SELECT YEAR(transaction_time) AS year 
    , MONTH(transaction_time) AS month 
    , sub_category
    , COUNT(transaction_id) AS num_trans
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id 
WHERE status_id = 1
AND category = 'Billing'
GROUP BY YEAR(transaction_time), MONTH(transaction_time), sub_category
ORDER BY year, month, sub_category



--PIVOT TABLE
WITH sub_cat_table AS (
SELECT YEAR(transaction_time) AS year 
    , MONTH(transaction_time) AS month 
    , sub_category
    , COUNT(transaction_id) AS num_trans
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id 
WHERE status_id = 1
AND category = 'Billing'
GROUP BY YEAR(transaction_time), MONTH(transaction_time), sub_category
)
SELECT year, month 
    , SUM(CASE WHEN sub_category='Electricity' THEN num_trans ELSE 0 END) AS electricity_trans
    , SUM(CASE WHEN sub_category='Internet' THEN num_trans ELSE 0 END) AS internet_trans
    , SUM(CASE WHEN sub_category='Water' THEN num_trans ELSE 0 END) AS water_trans 
FROM sub_cat_table
GROUP BY year, month 
ORDER BY year, month

--Percent of Total Calculations:
WITH sub_cat_table AS (
SELECT YEAR(transaction_time) AS year 
    , MONTH(transaction_time) AS month 
    , sub_category
    , COUNT(transaction_id) AS num_trans
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id 
WHERE status_id = 1
AND category = 'Billing'
GROUP BY YEAR(transaction_time), MONTH(transaction_time), sub_category
)
, num_sub_table AS (
SELECT year, month 
    , SUM(CASE WHEN sub_category='Electricity' THEN num_trans ELSE 0 END) AS electricity_trans
    , SUM(CASE WHEN sub_category='Internet' THEN num_trans ELSE 0 END) AS internet_trans
    , SUM(CASE WHEN sub_category='Water' THEN num_trans ELSE 0 END) AS water_trans 
FROM sub_cat_table
GROUP BY year, month
)
SELECT *
    , (electricity_trans + internet_trans + water_trans) AS total_trans_month
    , FORMAT(1.0*electricity_trans/(electricity_trans + internet_trans + water_trans), 'p') AS electricity_pct
    , FORMAT(1.0*internet_trans/(electricity_trans + internet_trans + water_trans), 'p') AS internet_pct
    , FORMAT(1.0*water_trans/(electricity_trans + internet_trans + water_trans), 'p') AS water_pct
FROM num_sub_table 
ORDER BY year, month

--Indexing to see Percent Change over time
WITH customer_table AS (
SELECT YEAR(transaction_time) AS year 
     , MONTH(transaction_time) AS month 
     , COUNT(DISTINCT customer_id) AS num_customers 
FROM 
(SELECT * FROM fact_transaction_2019
UNION
SELECT * FROM fact_transaction_2020) AS fact_table
LEFT JOIN dim_scenario ON fact_table.scenario_id = dim_scenario.scenario_id 
WHERE status_id = 1
AND sub_category IN ('Electricity', 'Internet', 'Water')
GROUP BY YEAR(transaction_time), MONTH(transaction_time)
)
SELECT *
    , FIRST_VALUE(num_customers) OVER (ORDER BY year, month) AS starting_point 
    , FORMAT(1.0*num_customers/FIRST_VALUE(num_customers) OVER (ORDER BY year, month)-1, 'p') AS difference_pct
FROM customer_table
ORDER BY year, month



--Task 2: Rolling Time Windows

WITH cus_table AS (
SELECT YEAR(transaction_time) AS year 
     , DATEPART(week, transaction_time) AS week 
     , COUNT(DISTINCT customer_id) AS num_customers
FROM 
(SELECT * FROM fact_transaction_2019
UNION
SELECT * FROM fact_transaction_2020) AS fact_table
LEFT JOIN dim_scenario ON fact_table.scenario_id = dim_scenario.scenario_id 
WHERE status_id = 1
AND sub_category IN ('Electricity', 'Internet', 'Water')
GROUP BY YEAR(transaction_time), DATEPART(week, transaction_time)
)

SELECT *
    , SUM(num_customers) OVER (PARTITION BY year ORDER BY year, week) AS rolling_users --POWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
FROM cus_table