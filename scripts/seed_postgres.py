import random
import psycopg2
from faker import Faker

fake = Faker()

DB_CONFIG = {
    "dbname": "ecommerce",
    "user": "userprod",
    "password": "passwordprod",
    "host": "localhost",
    "port": "5435",
}


# ---------------------------------------------------------------------------
# Generators — PURE functions only. No DB access here.
# Each takes whatever parent data it needs as arguments (via `depends_on`
# below) and returns a list of column-value lists, in the same column order
# you'll declare in SEED_PLAN.
# ---------------------------------------------------------------------------

def gen_customers():
    # return a list of [name, email] rows
    customers_list = []
    for _ in range(50):
        customers_list.append([
            fake.name(),
            fake.email(),
        ])
    return customers_list
    

def gen_products():
    # return a list of [name, category, price] rows
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
    # `customers` is whatever seed_table() stored for the "customers" step
    # (see context/store_result below) — decide what shape that should be.
    # return a list of [customer_id, order_date, status] rows
    orders_list = []
    for _ in range(200):
        orders_list.append([
            random.choice(customers),
            fake.date_time_between(start_date='-1y', end_date='now'),
            random.choice(['completed', 'failed', 'processed'])
        ])
    return orders_list


def gen_order_items(orders, products):
    # `orders` / `products` are whatever seed_table() stored for those steps.
    # return a list of [order_id, product_id, quantity, unit_price] rows
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


# ---------------------------------------------------------------------------
# Generic insert helper — table-agnostic, no knowledge of "customers" etc.
# ---------------------------------------------------------------------------

def insert_and_get_ids(conn, table, columns, id_column, rows):
    # insert each row, RETURNING id_column, collect and return the ids
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


# ---------------------------------------------------------------------------
# Declarative seed plan — describes WHAT to seed and in WHAT order,
# instead of main() hand-wiring variables between calls.
#
# `depends_on` lists other step names whose *stored context* this step's
# generate function needs — decide what you want stored per step (just ids?
# ids + rows so you can look up prices later?) and shape store_result()
# and each generator's signature to match.
# ---------------------------------------------------------------------------

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
    # decide what to keep in `context[table_name]` for later steps to consume
    # (e.g. just ids? ids zipped with rows? a dict keyed by id?)
    if table_name == "products":
        context[table_name] = {product_id: row[2] for product_id, row in zip(ids, rows)}
    else:
        context[table_name] = ids


def run_seed_plan(conn, plan):
    context = {}
    for step in plan:
        # 1. gather this step's dependencies out of `context`
        args = [context[dep] for dep in step["depends_on"]]
        # 2. call step["generate"](...) with those dependencies
        rows = step["generate"](*args)
        # 3. insert_and_get_ids(...) to persist and get back real ids
        ids = insert_and_get_ids(conn, step["table"], step["columns"], step["id_column"], rows)
        # 4. store_result(...) into context under step["table"]
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
