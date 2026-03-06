import pandas as pd
import numpy as np
from faker import Faker
import boto3
import random
from datetime import datetime, timedelta
import os

fake = Faker('en_AU')
random.seed(42)
np.random.seed(42)

SHOPS = [
    {'id': 'SHOP001', 'name': 'Sydney Fresh Groceries',    'city': 'Sydney',    'category': 'Grocery'},
    {'id': 'SHOP002', 'name': 'Melbourne Fashion Hub',     'city': 'Melbourne', 'category': 'Clothing'},
    {'id': 'SHOP003', 'name': 'Brisbane Electronics Plus', 'city': 'Brisbane',  'category': 'Electronics'},
    {'id': 'SHOP004', 'name': 'Perth Home Living',         'city': 'Perth',     'category': 'Furniture'},
    {'id': 'SHOP005', 'name': 'Adelaide Sports World',     'city': 'Adelaide',  'category': 'Sports'},
]

PRODUCTS = {
    'Grocery':     [('Organic Milk',2,6),('Bread Loaf',2,5),('Free Range Eggs',4,8),('Coffee Beans',12,25),('Olive Oil',8,20)],
    'Clothing':    [('Blue Jeans',49,120),('Cotton T-Shirt',20,60),('Winter Jacket',80,200),('Running Shoes',60,150),('Summer Dress',40,100)],
    'Electronics': [('Phone Case',10,40),('USB Cable',8,25),('Bluetooth Speaker',30,120),('Screen Protector',8,30),('Power Bank',25,80)],
    'Furniture':   [('Coffee Table',150,400),('Bookshelf',80,250),('Desk Lamp',30,90),('Office Chair',100,350),('Wall Mirror',50,200)],
    'Sports':      [('Yoga Mat',25,80),('Water Bottle',15,45),('Running Shoes',60,150),('Gym Gloves',15,50),('Resistance Bands',10,40)],
}

def get_multiplier(date):
    multiplier = 1.0
    if date.weekday() == 5:
        multiplier *= 1.4
    if date.weekday() == 6:
        multiplier *= 1.3
    if date.month == 12 and date.day >= 15:
        multiplier *= 1.8
    if date.month == 4:
        multiplier *= 1.2
    return multiplier

def generate_transactions():
    all_transactions = []
    transaction_id = 1
    start_date = datetime(2025, 3, 1)
    end_date = datetime(2026, 3, 1)

    for shop in SHOPS:
        current_date = start_date
        while current_date <= end_date:
            multiplier = get_multiplier(current_date)
            daily_transactions = int(np.random.poisson(6) * multiplier)
            products = PRODUCTS[shop['category']]

            for _ in range(daily_transactions):
                hour = np.random.choice(range(8,21),
                       p=[0.05,0.08,0.12,0.15,0.12,0.10,0.10,0.08,0.08,0.06,0.04,0.01,0.01])
                minute = random.randint(0, 59)
                product = random.choice(products)
                quantity = random.randint(1, 4)
                price = round(random.uniform(product[1], product[2]), 2)
                total = round(price * quantity, 2)

                all_transactions.append({
                    'transaction_id': f'TXN{transaction_id:06d}',
                    'date': current_date.strftime('%Y-%m-%d'),
                    'time': f'{hour:02d}:{minute:02d}',
                    'shop_id': shop['id'],
                    'shop_name': shop['name'],
                    'city': shop['city'],
                    'product_name': product[0],
                    'category': shop['category'],
                    'quantity': quantity,
                    'unit_price': price,
                    'total_amount': total,
                    'customer_id': f'CUST{random.randint(1,500):04d}',
                    'payment_method': random.choice(['card','cash','card','card']),
                })
                transaction_id += 1
            current_date += timedelta(days=1)

    return pd.DataFrame(all_transactions)

def upload_to_s3(df):
    s3 = boto3.client('s3', region_name='ap-southeast-2')
    bucket = 'diq-lakehouse-prod'

    for shop in SHOPS:
        shop_df = df[df['shop_id'] == shop['id']]
        filename = f"/tmp/{shop['id']}_transactions.csv"
        shop_df.to_csv(filename, index=False)
        s3_key = f"raw/transactions/{shop['id']}_transactions.csv"
        s3.upload_file(filename, bucket, s3_key)
        print(f"Uploaded {shop['id']} — {len(shop_df)} transactions")

if __name__ == '__main__':
    print("Generating transactions...")
    df = generate_transactions()
    print(f"Total transactions: {len(df)}")
    print("Uploading to S3...")
    upload_to_s3(df)
    print("Done! Check s3://diq-lakehouse-prod/raw/transactions/")
