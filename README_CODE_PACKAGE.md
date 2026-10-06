# Current Project Code Package

This package reflects the final architecture used in the working project.

Important:
- `databricks/my_transformation.py` is based on the latest transformation source retrieved from the conversation files, with the final negative-latency clamp applied.
- `producer_raw.py` and `dags/start_kafka_producer.py` are the retrieved source files.
- `docker-compose.yml`, `.env`, `requirements.txt`, and `databricks/00_setup.sql` reflect the verified working configuration.
- `databricks/Kafka_Pipeline_Quality_Check.sql` is a clean reconstruction of the validation checks shown in the project run/results. The original QC notebook source itself was not surfaced in the available file records.
- Current ngrok endpoint in this package is `0.tcp.in.ngrok.io:10037`. If ngrok gives a new endpoint, update `.env`, recreate the Kafka broker if required, and update `KAFKA_BOOTSTRAP_SERVERS` in `my_transformation.py`.

Final architecture:
RandomUser API → Airflow trigger once → continuous producer → Kafka → ngrok → Databricks → Bronze → Silver → Gold.

Do not use `docker compose down -v` for normal shutdown.
