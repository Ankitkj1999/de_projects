"""
Olist Brazilian E-Commerce Synthetic Generator
Generates clean baseline datasets for the Data Quality portfolio.
Maintains realistic relational integrity, timestamps, and distributions.
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Set fixed seeds for deterministic reproducibility
random.seed(42)
np.random.seed(42)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw")
CORRUPT_DIR = os.path.join(BASE_DIR, "corrupt")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CORRUPT_DIR, exist_ok=True)

BRAZIL_STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "GO"]
CITIES_BY_STATE = {
    "SP": ["Sao Paulo", "Campinas", "Santos", "Sao Bernardo do Campo"],
    "RJ": ["Rio de Janeiro", "Niteroi", "Petropolis"],
    "MG": ["Belo Horizonte", "Uberlandia", "Juiz de Fora"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringa"],
    "SC": ["Florianopolis", "Joinville", "Blumenau"],
    "BA": ["Salvador", "Feira de Santana"],
    "PE": ["Recife", "Olinda"],
    "CE": ["Fortaleza"],
    "GO": ["Goiania"],
}

CATEGORIES = [
    "beleza_saude",
    "informatica_acessorios",
    "cama_mesa_banho",
    "relogios_presentes",
    "telefonia",
    "automotivo",
    "brinquedos",
    "esporte_lazer",
    "moveis_decoracao",
    "utilidades_domesticas",
]

PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card"]
ORDER_STATUSES = ["delivered", "shipped", "invoiced", "processing", "approved", "canceled"]


def generate_clean_olist_dataset(num_orders=2500, num_products=200, num_customers=1000):
    print(f"Generating clean Olist dataset ({num_orders} orders, {num_products} products, {num_customers} customers)...")
    
    # 1. Customers
    customer_rows = []
    customer_ids = []
    for _ in range(num_customers):
        c_id = uuid.uuid4().hex[:32]
        c_unique_id = uuid.uuid4().hex[:32]
        state = random.choice(BRAZIL_STATES)
        city = random.choice(CITIES_BY_STATE[state])
        zip_prefix = random.randint(1000, 99999)
        customer_ids.append(c_id)
        customer_rows.append({
            "customer_id": c_id,
            "customer_unique_id": c_unique_id,
            "customer_zip_code_prefix": zip_prefix,
            "customer_city": city,
            "customer_state": state,
        })
    df_customers = pd.DataFrame(customer_rows)
    df_customers.to_csv(os.path.join(RAW_DIR, "olist_customers_dataset.csv"), index=False)

    # 2. Products
    product_rows = []
    product_ids = []
    product_prices = {}
    for _ in range(num_products):
        p_id = uuid.uuid4().hex[:32]
        category = random.choice(CATEGORIES)
        product_ids.append(p_id)
        # Baseline price per product
        base_price = round(random.uniform(15.0, 450.0), 2)
        product_prices[p_id] = base_price
        product_rows.append({
            "product_id": p_id,
            "product_category_name": category,
            "product_name_lenght": random.randint(20, 60),
            "product_description_lenght": random.randint(100, 2000),
            "product_photos_qty": random.randint(1, 8),
            "product_weight_g": random.randint(100, 8000),
            "product_length_cm": random.randint(10, 80),
            "product_height_cm": random.randint(5, 50),
            "product_width_cm": random.randint(10, 60),
        })
    df_products = pd.DataFrame(product_rows)
    df_products.to_csv(os.path.join(RAW_DIR, "olist_products_dataset.csv"), index=False)

    # 3. Orders, Items, Payments
    order_rows = []
    item_rows = []
    payment_rows = []

    # Historical timeline over past 90 days ending yesterday
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=90)

    for i in range(num_orders):
        order_id = uuid.uuid4().hex[:32]
        customer_id = random.choice(customer_ids)
        
        # Order timeline
        purchase_time = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # 92% delivered, 4% shipped, 2% invoiced, 2% canceled
        status_prob = random.random()
        if status_prob < 0.92:
            status = "delivered"
        elif status_prob < 0.96:
            status = "shipped"
        elif status_prob < 0.98:
            status = "invoiced"
        else:
            status = "canceled"

        approved_time = purchase_time + timedelta(minutes=random.randint(5, 120))
        estimated_delivery = purchase_time + timedelta(days=random.randint(5, 18))

        delivered_carrier_time = None
        delivered_customer_time = None

        if status in ["delivered", "shipped"]:
            delivered_carrier_time = approved_time + timedelta(hours=random.randint(6, 48))
        if status == "delivered":
            # Realistic delivery 2 to 10 days after carrier pickup
            delivered_customer_time = delivered_carrier_time + timedelta(days=random.randint(2, 10))

        order_rows.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": status,
            "order_purchase_timestamp": purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at": approved_time.strftime("%Y-%m-%d %H:%M:%S") if approved_time else "",
            "order_delivered_carrier_date": delivered_carrier_time.strftime("%Y-%m-%d %H:%M:%S") if delivered_carrier_time else "",
            "order_delivered_customer_date": delivered_customer_time.strftime("%Y-%m-%d %H:%M:%S") if delivered_customer_time else "",
            "order_estimated_delivery_date": estimated_delivery.strftime("%Y-%m-%d %H:%M:%S"),
        })

        # Items (1 to 4 items per order)
        num_items = random.choices([1, 2, 3, 4], weights=[0.75, 0.18, 0.05, 0.02])[0]
        order_total = 0.0
        seller_id = uuid.uuid4().hex[:32]

        for item_seq in range(1, num_items + 1):
            prod_id = random.choice(product_ids)
            price = product_prices[prod_id]
            freight = round(random.uniform(9.50, 45.00), 2)
            shipping_limit = approved_time + timedelta(days=random.randint(3, 7))

            item_rows.append({
                "order_id": order_id,
                "order_item_id": item_seq,
                "product_id": prod_id,
                "seller_id": seller_id,
                "shipping_limit_date": shipping_limit.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price,
                "freight_value": freight,
            })
            order_total += (price + freight)

        # Payment (matches item price + freight total)
        order_total = round(order_total, 2)
        payment_type = random.choice(PAYMENT_TYPES)
        installments = 1 if payment_type in ["boleto", "debit_card"] else random.randint(1, 10)
        
        payment_rows.append({
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": payment_type,
            "payment_installments": installments,
            "payment_value": order_total,
        })

    df_orders = pd.DataFrame(order_rows)
    df_items = pd.DataFrame(item_rows)
    df_payments = pd.DataFrame(payment_rows)

    df_orders.to_csv(os.path.join(RAW_DIR, "olist_orders_dataset.csv"), index=False)
    df_items.to_csv(os.path.join(RAW_DIR, "olist_order_items_dataset.csv"), index=False)
    df_payments.to_csv(os.path.join(RAW_DIR, "olist_order_payments_dataset.csv"), index=False)

    print(f"Generated clean dataset:")
    print(f" - Orders:   {len(df_orders)} rows -> {os.path.join(RAW_DIR, 'olist_orders_dataset.csv')}")
    print(f" - Items:    {len(df_items)} rows -> {os.path.join(RAW_DIR, 'olist_order_items_dataset.csv')}")
    print(f" - Payments: {len(df_payments)} rows -> {os.path.join(RAW_DIR, 'olist_order_payments_dataset.csv')}")
    print(f" - Products: {len(df_products)} rows -> {os.path.join(RAW_DIR, 'olist_products_dataset.csv')}")
    print(f" - Customers:{len(df_customers)} rows -> {os.path.join(RAW_DIR, 'olist_customers_dataset.csv')}")


if __name__ == "__main__":
    generate_clean_olist_dataset()

