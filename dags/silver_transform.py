from datetime import datetime
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

TABLES = ["customers", "products", "orders", "order_items"]

with DAG(
    dag_id="silver_transform",
    description="Transform the bronze layer into cleaned tables",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "medallion", "ecommerce_data"]
) as dag:
    for table in TABLES:
        if table == "customers":
            TrinoOperator(
                task_id=f"transform_{table}",
                trino_conn_id="trino_default",
                sql=f"""
                    CREATE TABLE iceberg.silver.customers AS
                    SELECT
                        customer_id,
                        name,
                        LOWER(email) AS email,
                        created_at
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (PARTITION BY LOWER(email) ORDER BY customer_id) AS rn
                        FROM iceberg.bronze.customers
                    )
                    WHERE rn = 1
                """,
                handler=list,
            )
        if table == "products":
            TrinoOperator(
                task_id=f"transform_{table}",
                trino_conn_id="trino_default",
                sql=f"""
                    CREATE TABLE iceberg.silver.products AS
                    SELECT 
                        product_id, 
                        name, 
                        category, 
                        price
                    FROM iceberg.bronze.products 
                    WHERE price > 0
                """,
                handler=list,
            )
        if table == "orders":
            TrinoOperator(
                task_id=f"transform_{table}",
                trino_conn_id="trino_default",
                sql=f"""
                    CREATE TABLE iceberg.silver.orders AS
                    SELECT
                        order_id,
                        customer_id,
                        CAST(order_date AS DATE) AS order_date,
                        status
                    FROM iceberg.bronze.orders
                    WHERE status IN ('completed', 'processed', 'failed')
                """,
                handler=list,
            )
        if table == "order_items":
            TrinoOperator(
                task_id=f"transform_{table}",
                trino_conn_id="trino_default",
                sql=f"""
                    CREATE TABLE iceberg.silver.order_items AS
                    SELECT 
                        order_item_id,
                        order_id,
                        product_id,
                        quantity,
                        unit_price
                    FROM iceberg.bronze.order_items
                    WHERE quantity > 0
                """,
                handler=list,
            )