-- Please edit the sample below

CREATE OR REFRESH STREAMING TABLE
    raw_bronze
AS SELECT
    *,
    current_timestamp() AS processing_time,
    _metadata.file_name AS source_file
FROM STREAM READ_FILES(
    "${source}",
    format => "CSV",
    header => true
);


CREATE OR REFRESH STREAMING TABLE l_silver.bronze_silver
(
    CONSTRAINT valid_status EXPECT (http_status <> 201),
    CONSTRAINT valid_swap EXPECT (categoria_produto IN ('OPC', 'SWP')) ON VIOLATION DROP ROW
)
AS SELECT
    *
FROM STREAM raw_bronze;

CREATE OR REFRESH MATERIALIZED VIEW l_gold.silver_gold

AS SELECT
    campo_simples_proporcional,
    categoria_produto,
    COUNT(*) AS total_count,
    SUM(http_status) AS sum_http_status
FROM l_silver.bronze_silver
GROUP BY campo_simples_proporcional, categoria_produto;

CREATE OR REFRESH MATERIALIZED VIEW l_gold.rows_by_files
AS SELECT
    left(source_file, 10) AS source_file,
    COUNT(*) AS total_count
FROM l_silver.bronze_silver
GROUP BY source_file;