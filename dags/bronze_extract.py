from datetime import datetime

from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

TABLES = ["customers", "products", "orders", "order_items"]

with DAG(
    dag_id="bronze_extract",
    description="Extract source-postgres tables into Iceberg bronze layer via Trino",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "medallion", "ecommerce_data"],
) as dag:
    for table in TABLES:
        TrinoOperator(
            task_id=f"extract_{table}",
            trino_conn_id="trino_default",
            sql=f"""
                CREATE TABLE iceberg.bronze.{table} AS
                SELECT * FROM postgres.public.{table}
            """,
            handler=list,
        )
