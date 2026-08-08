"""
generate_seed_data.py
----------------------
Generates synthetic e-commerce data and inserts it into MySQL.
Intentionally injects "dirty" records (nulls, dupes, bad formats) so the
silver-layer cleaning logic has something real to do.

Usage:
    python generate_seed_data.py --host <rds-endpoint> --user admin --password *** \
        --database ecommerce --num-customers 500 --num-orders 2000
"""

import argparse
import random

import mysql.connector
from faker import Faker

fake = Faker()
LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
ORDER_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
CATEGORIES = {
    "Electronics": ["Laptops", "Phones", "Accessories"],
    "Home": ["Kitchen", "Furniture", "Decor"],
    "Apparel": ["Men", "Women", "Kids"],
    "Sports": ["Fitness", "Outdoor", "Team Sports"],
}


def get_connection(args):
    return mysql.connector.connect(
        host=args.host, user=args.user, password=args.password, database=args.database
    )


def seed_customers(cursor, n, dirty_rate=0.05):
    rows = []
    for _ in range(n):
        dirty = random.random() < dirty_rate
        first = fake.first_name()
        last = fake.last_name()
        email = None if dirty and random.random() < 0.3 else fake.email()
        rows.append((
            first,
            last if not dirty else last.upper(),          # inconsistent casing
            email,
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            fake.postcode(),
            "USA",
            random.choice(LOYALTY_TIERS),
        ))
    cursor.executemany(
        """INSERT INTO customers
           (first_name, last_name, email, address, city, state, postal_code, country, loyalty_tier)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        rows,
    )
    # inject a handful of exact duplicate customers to test dedup logic
    cursor.execute("SELECT customer_id FROM customers ORDER BY RAND() LIMIT 5")
    dupe_ids = [r[0] for r in cursor.fetchall()]
    for cid in dupe_ids:
        cursor.execute("SELECT * FROM customers WHERE customer_id=%s", (cid,))
        row = cursor.fetchone()
        if row:
            cursor.execute(
                """INSERT INTO customers
                   (first_name, last_name, email, address, city, state, postal_code, country, loyalty_tier)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                row[1:10],
            )


def seed_products(cursor, n):
    rows = []
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        rows.append((
            fake.unique.bothify(text="SKU-#####"),
            fake.catch_phrase(),
            category,
            subcategory,
            round(random.uniform(5, 800), 2),
        ))
    cursor.executemany(
        """INSERT INTO products (sku, product_name, category, subcategory, unit_price)
           VALUES (%s,%s,%s,%s,%s)""",
        rows,
    )


def seed_orders_and_items(cursor, num_orders, num_customers, num_products):
    for _ in range(num_orders):
        customer_id = random.randint(1, num_customers)
        order_date = fake.date_time_between(start_date="-180d", end_date="now")
        status = random.choice(ORDER_STATUSES)
        shipping = round(random.uniform(0, 25), 2)
        cursor.execute(
            """INSERT INTO orders (customer_id, order_status, order_date, shipping_cost)
               VALUES (%s,%s,%s,%s)""",
            (customer_id, status, order_date, shipping),
        )
        order_id = cursor.lastrowid

        for _ in range(random.randint(1, 4)):
            product_id = random.randint(1, num_products)
            qty = random.randint(1, 5)
            unit_price = round(random.uniform(5, 800), 2)
            discount = random.choice([0, 0, 0, 5, 10, 15])
            cursor.execute(
                """INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_pct)
                   VALUES (%s,%s,%s,%s,%s)""",
                (order_id, product_id, qty, unit_price, discount),
            )


def seed_inventory(cursor, num_products):
    rows = []
    for pid in range(1, num_products + 1):
        rows.append((pid, random.choice(["WH-EAST", "WH-WEST", "WH-CENTRAL"]),
                      random.randint(0, 500), random.randint(5, 50)))
    cursor.executemany(
        """INSERT INTO inventory (product_id, warehouse_code, quantity_on_hand, reorder_level)
           VALUES (%s,%s,%s,%s)""",
        rows,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default="ecommerce")
    parser.add_argument("--num-customers", type=int, default=500)
    parser.add_argument("--num-products", type=int, default=200)
    parser.add_argument("--num-orders", type=int, default=2000)
    args = parser.parse_args()

    conn = get_connection(args)
    cursor = conn.cursor()

    print("Seeding customers...")
    seed_customers(cursor, args.num_customers)
    print("Seeding products...")
    seed_products(cursor, args.num_products)
    print("Seeding orders + order_items...")
    seed_orders_and_items(cursor, args.num_orders, args.num_customers, args.num_products)
    print("Seeding inventory...")
    seed_inventory(cursor, args.num_products)

    conn.commit()
    cursor.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
