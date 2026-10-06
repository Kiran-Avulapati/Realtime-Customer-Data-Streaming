-- Databricks setup
CREATE CATALOG IF NOT EXISTS data_engineering_project;

CREATE SCHEMA IF NOT EXISTS data_engineering_project.kafka_pipeline;

CREATE VOLUME IF NOT EXISTS data_engineering_project.kafka_pipeline.checkpoints;
