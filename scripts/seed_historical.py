"""
Historical Data Seeder
Generates 30 days of realistic e-commerce data into Bronze tables.
Run after setup.sh to populate the pipeline with demo data.
"""

import psycopg2
import random
import json
import uuid
from datetime import datetime, timedelta, timezone
from faker import Faker

fake = Faker()

DB_CONFIG = dict(
    host     = "localhost",
    port     = 5432,
    dbname   = "ecommerce_dw",
    user     = "warehouse",
    password = "warehouse123",
)

CATEGORIES = ["electronics", "clothing", "books", "home_kitchen",
              "sports", "beauty", "toys", "grocery"]
PRODUCTS = {
    "electronics":  ["laptop_pro","wireless_earbuds","smart_watch","tablet_x","monitor_4k"],
    "clothing":     ["tshirt_plain","jeans_slim","hoodie_zip","sneakers_run","dress_floral"],
    "books":        ["python_book","ml_handbook","fiction_novel","cook_book","sci_fi_trilogy"],
    "home_kitchen": ["coffee_maker","instant_pot","air_fryer","blender_pro","knife_set"],
    "sports":       ["yoga_mat","resistance_bands","dumbbell_set","running_shoes","gym_bag"],
    "beauty":       ["face_wash","moisturizer","lipstick_set","perfume_oud","serum_vit_c"],
    "toys":         ["lego_city","barbie_doll","rc_car","puzzle_1000","board_game"],
    "grocery":      ["coffee_beans","green_tea","protein_bar","olive_oil","dark_chocolate"],
}
PRICES = {p: round(random.uniform(5, 499), 2)
          for cat in PRODUCTS.values() for p in cat}
EVENT_TYPES  = ["page_view", "page_view", "page_view", "add_to_cart", "add_to_cart", "purchase"]
DEVICES      = ["mobile", "desktop", "tablet"]
COUNTRIES    = ["India", "USA", "UK", "Germany", "Australia", "Canada"]
CITIES       = {"India":["Hyderabad","Mumbai","Delhi","Bangalore"],
                "USA":["New York","San Francisco","Austin","Seattle"],
                "UK":["London","Manchester","Birmingham"],
                "Germany":["Berlin","Munich","Hamburg"],
                "Australia":["Sydney","Melbourne"],
                "Canada":["Toronto","Vancouver"]}
PAYMENT_METHODS = ["credit_card","debit_card","upi","net_banking","paypal","cod","wallet"]
ORDER_STATUSES  = ["placed","confirmed","shipped","delivered","delivered","delivered","cancelled"]


def seed(days_back=30, events_per_day=500, orders_per_day=80):
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    now  = datetime.now(timezone.utc)

    total_events = 0
    total_orders = 0

    print(f"Seeding {days_back} days of data...")

    for day_offset in range(days_back, 0, -1):
        day_start = now - timedelta(days=day_offset)

        # ── Events ──
        events = []
        # Weekend boost
        dow = day_start.weekday()
        multiplier = 1.4 if dow >= 5 else 1.0
        n_events = int(events_per_day * multiplier * random.uniform(0.8, 1.2))

        for _ in range(n_events):
            cat     = random.choice(CATEGORIES)
            prod    = random.choice(PRODUCTS[cat])
            country = random.choice(COUNTRIES)
            city    = random.choice(CITIES[country])
            ts      = day_start + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            events.append((
                str(uuid.uuid4()),
                random.choice(EVENT_TYPES),
                f"user_{random.randint(1, 400):04d}",
                f"sess_{uuid.uuid4().hex[:8]}",
                prod,
                cat,
                PRICES.get(prod, round(random.uniform(10, 200), 2)),
                random.randint(1, 3),
                f"/products/{cat}/{prod}",
                random.choice(["google","direct","email","instagram","facebook",""]),
                random.choice(DEVICES),
                country,
                city,
                ts,
                json.dumps({}),
            ))

        cur.executemany("""
            INSERT INTO bronze.raw_events
              (event_id, event_type, user_id, session_id, product_id, category,
               price, quantity, page_url, referrer, device_type, country, city,
               ts, raw_payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, events)
        total_events += len(events)

        # ── Orders ──
        orders = []
        n_orders = int(orders_per_day * multiplier * random.uniform(0.7, 1.3))

        for _ in range(n_orders):
            country = random.choice(COUNTRIES)
            items = [
                {"product_id": random.choice(PRODUCTS[random.choice(CATEGORIES)]),
                 "qty": random.randint(1, 2),
                 "price": round(random.uniform(10, 300), 2)}
                for _ in range(random.randint(1, 4))
            ]
            total = sum(i["price"] * i["qty"] for i in items)
            disc  = random.choice([0, 0, 0, 5, 10, 15, 20])
            ts    = day_start + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            orders.append((
                f"ORD_{uuid.uuid4().hex[:10].upper()}",
                f"user_{random.randint(1, 400):04d}",
                random.choice(ORDER_STATUSES),
                round(total * (1 - disc / 100), 2),
                disc,
                random.choice(PAYMENT_METHODS),
                json.dumps(items),
                json.dumps({"city": random.choice(CITIES[country]),
                             "country": country}),
                ts,
                ts + timedelta(hours=random.randint(1, 48)),
                json.dumps({}),
            ))

        cur.executemany("""
            INSERT INTO bronze.raw_orders
              (order_id, user_id, status, total_amount, discount_pct,
               payment_method, items, shipping_addr, created_at, updated_at, raw_payload)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, orders)
        total_orders += len(orders)

        if day_offset % 5 == 0 or day_offset == 1:
            conn.commit()
            print(f"  Day -{day_offset:2d} seeded | "
                  f"cumulative: {total_events:,} events, {total_orders:,} orders")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✓ Seeding complete: {total_events:,} events | {total_orders:,} orders")


if __name__ == "__main__":
    seed()
