from datetime import datetime

from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

MARTS = {
    "daily_revenue": """
        CREATE TABLE iceberg.gold.daily_revenue AS
        SELECT
            o.order_date,
            SUM(oi.quantity * oi.unit_price) AS revenue,
            COUNT(DISTINCT o.order_id) AS order_count
        FROM iceberg.silver.orders o
        JOIN iceberg.silver.order_items oi ON oi.order_id = o.order_id
        WHERE o.status = 'completed'
        GROUP BY o.order_date
    """,
    "top_products": """
        CREATE TABLE iceberg.gold.top_products AS
        SELECT
            p.product_id,
            p.name,
            p.category,
            SUM(oi.quantity) AS units_sold,
            SUM(oi.quantity * oi.unit_price) AS revenue
        FROM iceberg.silver.order_items oi
        JOIN iceberg.silver.products p ON p.product_id = oi.product_id
        JOIN iceberg.silver.orders o ON o.order_id = oi.order_id
        WHERE o.status = 'completed'
        GROUP BY p.product_id, p.name, p.category
    """,
    "customer_ltv": """
        CREATE TABLE iceberg.gold.customer_ltv AS
        SELECT
            c.customer_id,
            c.name,
            c.email,
            COUNT(DISTINCT o.order_id) AS order_count,
            SUM(oi.quantity * oi.unit_price) AS lifetime_value
        FROM iceberg.silver.customers c
        JOIN iceberg.silver.orders o ON o.customer_id = c.customer_id
        JOIN iceberg.silver.order_items oi ON oi.order_id = o.order_id
        WHERE o.status = 'completed'
        GROUP BY c.customer_id, c.name, c.email
    """,
}

with DAG(
    dag_id="gold_marts",
    description="Aggregate the silver layer into business marts",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "medallion", "ecommerce_data"],
) as dag:
    for mart_name, sql in MARTS.items():
        TrinoOperator(
            task_id=f"build_{mart_name}",
            trino_conn_id="trino_default",
            sql=sql,
            handler=list,
        )
