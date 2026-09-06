import os
import random
import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

fake = Faker()

DB_CONFIG = {
    "dbname": os.environ["SOURCE_POSTGRES_DB"],
    "user": os.environ["SOURCE_POSTGRES_USER"],
    "password": os.environ["SOURCE_POSTGRES_PASSWORD"],
    "host": "localhost",
    "port": os.environ["SOURCE_POSTGRES_PORT"],
}

def gen_customers():

    customers_list = []
    for _ in range(50):
        customers_list.append([
            fake.name(),
            fake.email(),
        ])
    return customers_list
    

def gen_products():

    products_list = []
    for _ in range(30):
        products_list.append([
            fake.catch_phrase(),
            random.choice([
                    "Electronics",
                    "Fashion & Apparel",
                    "Home & Kitchen",
                    "Beauty & Personal Care",
                    "Sports & Outdoors",
                    "Books & Media",
                    "Toys & Games",
                    "Health & Wellness",
                    "Automotive",
                    "Groceries & Food"
                        ]),
            fake.pyfloat(left_digits=None, right_digits=2, min_value=10, max_value=500),
        ])
    return products_list


def gen_orders(customers):

    orders_list = []
    for _ in range(200):
        orders_list.append([
            random.choice(customers),
            fake.date_time_between(start_date='-1y', end_date='now'),
            random.choice(['completed', 'failed', 'processed'])
        ])
    return orders_list


def gen_order_items(orders, products):

    order_items_list = []
    for _ in range(500):
        product_id = random.choice(list(products.keys()))
        unit_price = products[product_id]
        order_items_list.append([
            random.choice(orders),
            product_id,
            random.randint(1, 10),
            unit_price
        ])
    return order_items_list


def insert_and_get_ids(conn, table, columns, id_column, rows):

    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    query = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) RETURNING {id_column}"

    ids = []
    with conn.cursor() as curs:
        for row in rows:
            curs.execute(query, row)
            ids.append(curs.fetchone()[0])
    conn.commit()
    return ids




SEED_PLAN = [
    {
        "table": "customers",
        "columns": ["name", "email"],
        "id_column": "customer_id",
        "depends_on": [],
        "generate": gen_customers,
    },
    {
        "table": "products",
        "columns": ["name", "category", "price"],
        "id_column": "product_id",
        "depends_on": [],
        "generate": gen_products,
    },
    {
        "table": "orders",
        "columns": ["customer_id", "order_date", "status"],
        "id_column": "order_id",
        "depends_on": ["customers"],
        "generate": gen_orders,
    },
    {
        "table": "order_items",
        "columns": ["order_id", "product_id", "quantity", "unit_price"],
        "id_column": "order_item_id",
        "depends_on": ["orders", "products"],
        "generate": gen_order_items,
    },
]


def store_result(context, table_name, rows, ids):

    if table_name == "products":
        context[table_name] = {product_id: row[2] for product_id, row in zip(ids, rows)}
    else:
        context[table_name] = ids


def run_seed_plan(conn, plan):
    context = {}
    for step in plan:

        args = [context[dep] for dep in step["depends_on"]]
        
        rows = step["generate"](*args)
        
        ids = insert_and_get_ids(conn, step["table"], step["columns"], step["id_column"], rows)
        
        store_result(context, step["table"], rows, ids)
        
    return context


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        run_seed_plan(conn, SEED_PLAN)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
