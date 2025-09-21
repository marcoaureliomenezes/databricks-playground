CREATE OR REFRESH STREAMING TABLE l_bronze.sm_customers
COMMENT "Ingest customer JSON files from cloud storage"
TBLPROPERTIES (
  "quality" = "bronze",
  "pipelines.reset.allowed" = true
)
AS SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}",
  format => 'json'
);



-- Tabela Silver com users tratados
CREATE OR REFRESH STREAMING TABLE l_silver.sm_customers_cleaned
  (
    CONSTRAINT valid_type_customer EXPECT (user_type IN ('standard', 'premium', 'gold')),
    CONSTRAINT age_greater_than_18 EXPECT (
      datediff(current_date(), birth_date) / 365 >= 18
    ),
    CONSTRAINT valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW -- FAIL UPDATE
  )
  COMMENT "Silver clean users table"
  TBLPROPERTIES ("quality" = "silver")
  AS SELECT
    user_id,
    user_type,
    concat_ws(' ', first_name, last_name) AS full_name,
    CAST(datediff(current_date(), to_date(birth_date, 'dd/MM/yyyy')) / 365 AS INT) AS age,
    income, balance,
    to_date(birth_date, 'dd/MM/yyyy') AS birth_date
    FROM STREAM l_bronze.sm_customers;


-- Materialized View com média 
CREATE OR REFRESH MATERIALIZED VIEW l_gold.customers_income_by_age
COMMENT "Aggregated gold data for averarage income and sum of debts"
TBLPROPERTIES ("quality" = "gold")
AS SELECT
  a.generation, a.user_type,
  ROUND(AVG(a.income), 2) AS income_avg,
  ROUND(AVG(a.balance), 2) AS balance_avg
  FROM (
    SELECT *,
    CASE WHEN age between 18 and 25 THEN 'Geração Z'
      WHEN age between 26 and 40 THEN 'Geração Y'
      WHEN age between 41 and 55 THEN 'Geração X'
      WHEN age between 56 and 70 THEN 'Baby Boomers'
      ELSE 'Veteranos'
      END AS generation
  FROM l_silver.sm_customers_cleaned) a
  GROUP BY user_type, generation
  ORDER BY generation;
