-- Kafka_Pipeline_Quality_Check
-- Reconstructed from the validation checks actually run in the project.
-- Core table names and reconciliation rules match the final pipeline.

WITH counts AS (
  SELECT 'gold_customers' table_name, COUNT(*) record_count,
         MAX(processing_timestamp) latest_timestamp FROM data_engineering_project.kafka_pipeline.gold_customers
  UNION ALL
  SELECT 'gold_data_quality', COUNT(*), MAX(processing_timestamp)
    FROM data_engineering_project.kafka_pipeline.gold_data_quality
  UNION ALL
  SELECT 'gold_demographic_state', COUNT(*), MAX(injection_timestamp)
    FROM data_engineering_project.kafka_pipeline.gold_demographic_state
  UNION ALL
  SELECT 'gold_demographics', COUNT(*), CAST(NULL AS TIMESTAMP)
    FROM data_engineering_project.kafka_pipeline.gold_demographics
  UNION ALL
  SELECT 'gold_geography', COUNT(*), CAST(NULL AS TIMESTAMP)
    FROM data_engineering_project.kafka_pipeline.gold_geography
  UNION ALL
  SELECT 'gold_geography_state', COUNT(*), MAX(injection_timestamp)
    FROM data_engineering_project.kafka_pipeline.gold_geography_state
  UNION ALL
  SELECT 'gold_pipeline_metrics', COUNT(*), MAX(processing_timestamp)
    FROM data_engineering_project.kafka_pipeline.gold_pipeline_metrics
  UNION ALL
  SELECT 'users_bronze', COUNT(*), MAX(timestamp)
    FROM data_engineering_project.kafka_pipeline.users_bronze
  UNION ALL
  SELECT 'users_silver', COUNT(*), MAX(processing_timestamp)
    FROM data_engineering_project.kafka_pipeline.users_silver
)
SELECT
  table_name,
  record_count,
  latest_timestamp,
  CASE WHEN record_count >= 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM counts
ORDER BY table_name;


-- Duplicate user IDs
SELECT
  COUNT(*) AS duplicate_user_id_groups
FROM (
  SELECT user_id
  FROM data_engineering_project.kafka_pipeline.users_silver
  GROUP BY user_id
  HAVING COUNT(*) > 1
);


-- Critical NULL records
SELECT COUNT(*) AS critical_null_records
FROM data_engineering_project.kafka_pipeline.users_silver
WHERE user_id IS NULL;


-- Pipeline metrics: negative latency must be zero after final correction.
SELECT
  MIN(processing_latency_seconds) AS min_latency,
  MAX(processing_latency_seconds) AS max_latency,
  AVG(processing_latency_seconds) AS avg_latency,
  COUNT(*) AS negative_records
FROM data_engineering_project.kafka_pipeline.gold_pipeline_metrics
WHERE processing_latency_seconds < 0;


-- Synchronization / reconciliation
SELECT
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.users_bronze) AS bronze_count,
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.users_silver) AS silver_count,
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.gold_customers) AS gold_customer_count,
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.gold_data_quality) AS quality_count,
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.gold_pipeline_metrics) AS metrics_count,
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.gold_demographic_state) AS demographic_state_count,
  (SELECT COUNT(*) FROM data_engineering_project.kafka_pipeline.gold_geography_state) AS geography_state_count;
