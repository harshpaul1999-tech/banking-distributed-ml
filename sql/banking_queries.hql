-- Run after copying data/bank.csv to HDFS at /user/<user>/banking/input/bank.csv.
CREATE DATABASE IF NOT EXISTS banking_data;
USE banking_data;

CREATE EXTERNAL TABLE IF NOT EXISTS client_info (
  age INT,
  job STRING,
  marital STRING,
  education STRING,
  credit_default STRING,
  balance BIGINT,
  housing STRING,
  personal_loan STRING,
  contact STRING,
  contact_day INT,
  campaign_month STRING,
  duration INT,
  campaign INT,
  pdays INT,
  previous INT,
  poutcome STRING,
  subscribed STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/<user>/banking/input'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- 1. Basic exploration
SELECT COUNT(*) AS client_count FROM client_info;
SELECT * FROM client_info LIMIT 10;

-- 2. Filtering and sorting
SELECT * FROM client_info
WHERE marital = 'married' AND personal_loan = 'yes';
SELECT job, marital, balance
FROM client_info
ORDER BY balance DESC
LIMIT 10;

-- 3. Aggregation
SELECT job, AVG(age) AS average_age, COUNT(*) AS clients
FROM client_info GROUP BY job ORDER BY average_age DESC;
SELECT education, COUNT(*) AS clients_with_credit_default
FROM client_info WHERE credit_default = 'yes'
GROUP BY education ORDER BY clients_with_credit_default DESC;

-- 4. Top jobs by balance, with subscription rate
SELECT job,
       AVG(balance) AS average_balance,
       COUNT(*) AS clients,
       100.0 * SUM(CASE WHEN subscribed = 'yes' THEN 1 ELSE 0 END) / COUNT(*) AS subscription_pct
FROM client_info
GROUP BY job
ORDER BY average_balance DESC
LIMIT 5;

-- 5. Busiest contact month and its campaign success rate
WITH monthly AS (
  SELECT campaign_month, COUNT(*) AS contacts,
         SUM(CASE WHEN subscribed = 'yes' THEN 1 ELSE 0 END) AS subscriptions
  FROM client_info GROUP BY campaign_month
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY contacts DESC) AS rn FROM monthly
)
SELECT campaign_month, contacts, subscriptions,
       100.0 * subscriptions / contacts AS subscription_pct
FROM ranked WHERE rn = 1;

-- 6. Cross-sectional correlation, not a time trend
SELECT CORR(CAST(age AS DOUBLE), CAST(balance AS DOUBLE)) AS age_balance_corr
FROM client_info;

-- 7. The file contains no year/date field, so year-over-year analysis cannot be
-- computed. This is the available within-dataset month-of-year contact profile.
SELECT campaign_month, COUNT(*) AS contacts,
       SUM(CASE WHEN subscribed = 'yes' THEN 1 ELSE 0 END) AS subscriptions
FROM client_info
GROUP BY campaign_month
ORDER BY CASE campaign_month
  WHEN 'jan' THEN 1 WHEN 'feb' THEN 2 WHEN 'mar' THEN 3 WHEN 'apr' THEN 4
  WHEN 'may' THEN 5 WHEN 'jun' THEN 6 WHEN 'jul' THEN 7 WHEN 'aug' THEN 8
  WHEN 'sep' THEN 9 WHEN 'oct' THEN 10 WHEN 'nov' THEN 11 WHEN 'dec' THEN 12
  ELSE 13 END;

-- 8. Compare average yearly balance by education. Each row is one client;
-- the standard-deviation score is descriptive, not a causal conclusion.
WITH education_stats AS (
  SELECT education, AVG(CAST(balance AS DOUBLE)) AS avg_balance, COUNT(*) AS clients
  FROM client_info GROUP BY education
), baseline AS (
  SELECT AVG(CAST(balance AS DOUBLE)) AS overall_avg,
         STDDEV_POP(CAST(balance AS DOUBLE)) AS overall_sd
  FROM client_info
)
SELECT e.education, e.clients, e.avg_balance, b.overall_avg,
       (e.avg_balance - b.overall_avg) / b.overall_sd AS balance_z_score
FROM education_stats e CROSS JOIN baseline b
ORDER BY ABS((e.avg_balance - b.overall_avg) / b.overall_sd) DESC;

-- 9. Previous campaign outcome and current outcome
SELECT poutcome, COUNT(*) AS clients,
       SUM(CASE WHEN subscribed = 'yes' THEN 1 ELSE 0 END) AS subscriptions,
       100.0 * SUM(CASE WHEN subscribed = 'yes' THEN 1 ELSE 0 END) / COUNT(*) AS subscription_pct,
       AVG(duration) AS average_duration_seconds
FROM client_info
GROUP BY poutcome
ORDER BY subscription_pct DESC;

SELECT subscribed, AVG(duration) AS average_duration_seconds,
       percentile_approx(duration, 0.5) AS median_duration_seconds
FROM client_info GROUP BY subscribed;
