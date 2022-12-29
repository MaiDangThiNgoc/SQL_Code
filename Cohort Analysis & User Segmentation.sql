--Task 1: Cohort Analysis
/*
As you know that 'Telco Card' is the most product in the Telco group (accounting for more than 99% of the total). 
You want to evaluate the quality of user acquisition in Jan 2019 by the retention metric. 
First, you need to know how many users are retained in each subsequent month from the first month (Jan 2019) they pay the successful transaction (only get data of 2019). 
*/

WITH retention_table AS (
SELECT customer_id
    , MONTH(transaction_time) AS present_month
    , MIN(MONTH(transaction_time)) OVER (PARTITION BY customer_id) AS first_purchase
    , DATEDIFF(month, MIN(transaction_time) OVER (PARTITION BY customer_id), transaction_time) AS subsequent_month --retention of purchase month (how many months from the first month)  
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND sub_category = 'Telco Card'
)
, retention_from_Jan AS (
SELECT present_month, first_purchase, subsequent_month
    , COUNT(DISTINCT customer_id) AS retained_users
FROM retention_table
WHERE first_purchase = 1 
GROUP BY present_month, first_purchase, subsequent_month
)
SELECT *
    , FIRST_VALUE(retained_users) OVER (ORDER BY subsequent_month) AS original_users 
    , FORMAT(1.0*retained_users/FIRST_VALUE(retained_users) OVER (ORDER BY subsequent_month), 'p') AS pct_retained
FROM retention_from_Jan
ORDER BY present_month, first_purchase, subsequent_month



--Task 2: Cohorts Derived form the Time Series itself
/* 
Expend the previous query to calculate retention for multi attributes from the acquisition month (from Jan to December)
*/

WITH customer_table AS (
SELECT customer_id 
    , MONTH(transaction_time) AS acquisition_month
    , MIN(MONTH(transaction_time)) OVER (PARTITION BY customer_id) AS first_month
    , DATEDIFF(month, MIN(transaction_time) OVER (PARTITION BY customer_id), transaction_time) AS subsequent_month 
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND sub_category = 'Telco Card'
)
, retained_table AS (
SELECT first_month, subsequent_month
    , COUNT(DISTINCT customer_id) AS retained_users
    , FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (PARTITION BY first_month ORDER BY subsequent_month) AS original_users
    , FORMAT(COUNT(DISTINCT customer_id)*1.0/ FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (PARTITION BY first_month ORDER BY subsequent_month), 'p') AS pct_retained
FROM customer_table
GROUP BY first_month, subsequent_month 
)
SELECT first_month, original_users
    , MIN(CASE WHEN subsequent_month=0 THEN pct_retained END) AS '0'
    , MIN(CASE WHEN subsequent_month=1 THEN pct_retained END) AS '1'
    , MIN(CASE WHEN subsequent_month=2 THEN pct_retained END) AS '2'
    , MIN(CASE WHEN subsequent_month=3 THEN pct_retained END) AS '3'
    , MIN(CASE WHEN subsequent_month=4 THEN pct_retained END) AS '4'
    , MIN(CASE WHEN subsequent_month=5 THEN pct_retained END) AS '5'
    , MIN(CASE WHEN subsequent_month=6 THEN pct_retained END) AS '6'
    , MIN(CASE WHEN subsequent_month=7 THEN pct_retained END) AS '7'
    , MIN(CASE WHEN subsequent_month=8 THEN pct_retained END) AS '8'
    , MIN(CASE WHEN subsequent_month=9 THEN pct_retained END) AS '9'
    , MIN(CASE WHEN subsequent_month=10 THEN pct_retained END) AS '10'
    , MIN(CASE WHEN subsequent_month=11 THEN pct_retained END) AS '11'
FROM retained_table
GROUP BY first_month, original_users
ORDER BY first_month




--Task 3: User Segmentation (RFM Segmentation)
/*
The first step in building an RFM model is to assign Recency, Frequency and Monetary values to each customer. 
Let’s calculate these metrics for all successful paying customer of ‘Telco Card’ in 2019 and 2020: 
●	Recency: Difference between each customer's last payment date and '2020-12-31'
●	Frequency: Number of successful payment DAYS of each customer
●	Monetary: Total charged amount of each customer 
*/


WITH customer_table AS (
SELECT customer_id
    , transaction_id
    , charged_amount
    , CONVERT(varchar, transaction_time, 101) AS transaction_time
FROM 
(SELECT * FROM fact_transaction_2019
UNION 
SELECT * FROM fact_transaction_2020) AS fact_table
LEFT JOIN dim_scenario ON fact_table.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND sub_category = 'Telco Card'
)
, rfm_table AS (
SELECT customer_id
    , DATEDIFF(day, MAX(transaction_time), '2020-12-31') AS recency
    , COUNT(DISTINCT transaction_time) AS frequency
    , SUM(1.0*charged_amount) AS monetary
FROM customer_table
GROUP BY customer_id
)
, rank_table AS (
SELECT *
    , PERCENT_RANK() OVER (ORDER BY recency ASC) AS r_rank
    , PERCENT_RANK() OVER (ORDER BY frequency DESC) AS f_rank
    , PERCENT_RANK() OVER (ORDER BY monetary DESC) AS m_rank
FROM rfm_table 
)
, rfm_tier AS (
SELECT *
    , CASE WHEN r_rank > 0.75 THEN 4
           WHEN r_rank > 0.5 THEN 3
           WHEN r_rank > 0.25 THEN 2
           ELSE 1 END AS r_tier 
    , CASE WHEN f_rank > 0.75 THEN 4
           WHEN f_rank > 0.5 THEN 3
           WHEN f_rank > 0.25 THEN 2
           ELSE 1 END AS f_tier 
    , CASE WHEN m_rank > 0.75 THEN 4
           WHEN m_rank > 0.5 THEN 3
           WHEN m_rank > 0.25 THEN 2
           ELSE 1 END AS m_tier
FROM rank_table
)
, rfm_group AS (
SELECT *
    , CONCAT(r_tier, f_tier, m_tier) AS rfm_score
FROM rfm_tier
)
, segment_table AS (
SELECT *
    , CASE WHEN rfm_score = 111 THEN 'Best Customers'
           WHEN rfm_score LIKE '[3-4][3-4][1-4]' THEN 'Lost Bad Customer'
           WHEN rfm_score LIKE '[3-4]2[1-4]' THEN 'Lost Customers'
           WHEN rfm_score LIKE '21[1-4]' THEN 'Almost Lost'
           WHEN rfm_score LIKE '11[2-4]' THEN 'Loyal Customers'
           WHEN rfm_score LIKE '[1-2][1-3]1' THEN 'Big Spenders'
           WHEN rfm_score LIKE '[1-2]4[1-4]' THEN 'New Customers'
           WHEN rfm_score LIKE '[3-4]1[1-4]' THEN 'Hibernating'
           WHEN rfm_score LIKE '[1-2][2-3][2-4]' THEN 'Potential Loyalists'
      ELSE 'unknown' END AS segment
FROM rfm_group
)
SELECT segment
    , COUNT(customer_id) AS number_users 
    , SUM(COUNT(customer_id)) OVER () AS total_users
    , FORMAT(1.0*COUNT(customer_id)/SUM(COUNT(customer_id)) OVER (), 'p') AS pct  
FROM segment_table
GROUP BY segment
ORDER BY number_users DESC