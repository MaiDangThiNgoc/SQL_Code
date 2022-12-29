/*
Paytm is an Indian multinational financial technology company. It specializes in digital payment system, e-commerce and financial services. 
Paytm wallet is a secure and RBI (Reserve Bank of India)-approved digital/mobile wallet that provides a myriad of financial features to fulfill every consumer’s payment needs.
Paytm wallet can be topped up through UPI (Unified Payments Interface), internet banking, or credit/debit cards. 
Users can also transfer money from a Paytm wallet to recipient’s bank account or their own Paytm wallet. 
Below is a small database of payment transactions from 2019 to 2020 of Paytm Wallet. The database includes 6 tables: 
●	fact_transaction (2019 & 2020): Store information of all types of transactions: Payments, Top-up, Transfers, Withdrawals
●	dim_scenario: Detailed description of transaction types
●	dim_payment_channel: Detailed description of payment methods
●	dim_platform: Detailed description of payment devices
●	dim_status: Detailed description of the results of the transaction
*/

--Task 1: Retrieve an overview report of payment types
--Task 1.1:

WITH tran_table AS (
SELECT transaction_type
    , COUNT(transaction_id) AS num_trans
    , SUM(COUNT(transaction_id)) OVER() AS total_num_trans
    --, (SELECT COUNT(transaction_id) FROM fact_transaction_2019 WHERE status_id=1) AS total_num_trans
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
GROUP BY transaction_type
)
SELECT TOP 5 * 
    , FORMAT(1.0*num_trans/total_num_trans, 'p') AS pct
FROM tran_table
ORDER BY pct DESC


--Task 1.2: 
WITH tran_table AS (
SELECT transaction_type, category
    , COUNT(transaction_id) AS num_trans
    , SUM(COUNT(transaction_id)) OVER (PARTITION BY transaction_type) AS total_num_trans --ROW BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
GROUP BY transaction_type, category
)
SELECT *
    , FORMAT(1.0*num_trans/total_num_trans, 'p') AS pct_type
FROM tran_table
ORDER BY transaction_type, category


--Task 2: Retrieve an overview report of customer's payment behaviors
/*
Task 2.1: Paytm has acquired a lot of customers. Retrieve a report that includes the following information: the number of transactions, the number of payment scenarios, the number of payment category and the total of charged amount of each customer.
●	Were created in 2019
●	Had status description is successful
●	Had transaction type is payment
●	Only show Top 10 highest customers by the number of transactions
*/

SELECT TOP 10 customer_id
    , COUNT(transaction_id) AS num_trans
    , COUNT(DISTINCT fact_transaction_2019.scenario_id) AS num_scen
    , COUNT(DISTINCT category) AS num_category
    , SUM(charged_amount) AS total_amount
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND transaction_type = 'Payment'
GROUP BY customer_id
ORDER BY num_trans DESC


/* 2.2.	After looking at the above metrics of customer’s payment behaviors, we want to analyze the distribution of each metric. 
Before calculating and plotting the distribution to check the frequency of values in each metric, we need to group the observations into range.
Based on the result of 2.1, retrieve a report that includes the following columns: metric, minimum value, maximum value and average value of these metrics: 
●	The total charged amount
●	The number of transactions
●	The number of payment scenarios
●	The number of payment categories
*/

WITH customer_table AS (
SELECT customer_id 
    , COUNT(transaction_id) AS num_trans
    , COUNT(DISTINCT fact_transaction_2019.scenario_id) AS num_scen
    , COUNT(DISTINCT category) AS num_category
    , SUM(1.0*charged_amount) AS total_amount
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND transaction_type = 'Payment'
GROUP BY customer_id
)
SELECT metric = 'The number of transaction'
    , min_value = MIN(num_trans)
    , max_value = MAX(num_trans)
    , avg_value = AVG(num_trans)
FROM customer_table
UNION
SELECT metric = 'The number of scenarios'
    , min_value = MIN(num_scen)
    , max_value = MAX(num_scen)
    , avg_value = AVG(num_scen)
FROM customer_table
UNION
SELECT metric = 'The number of categories'
    , min_value = MIN(num_category)
    , max_value = MAX(num_category)
    , avg_value = AVG(num_category)
FROM customer_table
UNION 
SELECT metric = 'The total charged amount'
    , min_value = MIN(total_amount)
    , max_value = MAX(total_amount)
    , avg_value = AVG(total_amount)
FROM customer_table

--Calculate the frequency of each field in each metric
--Metric 1: The number of payment categories
WITH customer_table AS (
SELECT customer_id 
    , COUNT(transaction_id) AS num_trans
    , COUNT(DISTINCT fact_transaction_2019.scenario_id) AS num_scen
    , COUNT(DISTINCT category) AS num_category
    , SUM(charged_amount) AS total_amount
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND transaction_type = 'Payment'
GROUP BY customer_id --customer_id is unique
)
SELECT num_category
    , COUNT(customer_id) AS num_customers
FROM customer_table
GROUP BY num_category
ORDER BY num_category

--Metric 2: The number of payment scenario

WITH customer_table AS (
SELECT customer_id 
    , COUNT(transaction_id) AS num_trans
    , COUNT(DISTINCT fact_transaction_2019.scenario_id) AS num_scen
    , COUNT(DISTINCT category) AS num_category
    , SUM(charged_amount) AS total_amount
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND transaction_type = 'Payment'
GROUP BY customer_id --customer_id is unique
)
SELECT num_scen
    , COUNT(customer_id) AS num_customers
FROM customer_table
GROUP BY num_scen
ORDER BY num_scen

--Metric 3: The total charged amount
WITH customer_table AS (
SELECT customer_id 
    , COUNT(transaction_id) AS num_trans
    , COUNT(DISTINCT fact_transaction_2019.scenario_id) AS num_scen
    , COUNT(DISTINCT category) AS num_category
    , SUM(charged_amount) AS total_amount
FROM fact_transaction_2019
LEFT JOIN dim_scenario ON fact_transaction_2019.scenario_id = dim_scenario.scenario_id
WHERE status_id = 1
AND transaction_type = 'Payment'
GROUP BY customer_id --customer_id is unique
)
, label_table AS (
SELECT customer_id
    , total_amount
    , CASE WHEN total_amount <= 1000000 THEN '0-01M'
           WHEN total_amount <= 2000000 THEN '01-02M'
           WHEN total_amount <= 3000000 THEN '02-03M'
           WHEN total_amount <= 4000000 THEN '03-04M'
           WHEN total_amount <= 5000000 THEN '04-05M'
           WHEN total_amount <= 6000000 THEN '05-06M'
           WHEN total_amount <= 7000000 THEN '06-07M'
           WHEN total_amount <= 8000000 THEN '07-08M'
           WHEN total_amount <= 9000000 THEN '08-09M'
           WHEN total_amount <= 10000000 THEN '09-10M'
    ELSE '>10M' END AS charged_amount_range
FROM customer_table
)
SELECT charged_amount_range
    , COUNT(customer_id) AS num_customers
FROM label_table
GROUP BY charged_amount_range
ORDER BY charged_amount_range


