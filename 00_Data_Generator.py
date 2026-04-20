# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 🌊 Data Generator — Simulated Live Feed
# MAGIC ## 🍕 FoodRush India | Food Delivery Orders Pipeline
# MAGIC ---
# MAGIC ### What this notebook does:
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1 | Define the landing zone path |
# MAGIC | 2 | Define a pool of realistic FoodRush India order records |
# MAGIC | 3 | Every **10 seconds**, pick 5 random records and write a new CSV file |
# MAGIC | 4 | Each file is uniquely named using a timestamp |
# MAGIC | 5 | Run indefinitely until you **stop the cell manually** |
# MAGIC
# MAGIC > 🔑 **Purpose:** This notebook simulates a real-time order feed arriving in the landing zone.
# MAGIC > Run this notebook **first**, then run the Bronze streaming notebook alongside it.
# MAGIC
# MAGIC > ⚠️ **How to stop:** Click the **Interrupt** (■) button on the running cell. The stream will stop cleanly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📁 Step 1: Define Landing Zone Path

# COMMAND ----------

raw_landing_path = "dbfs:/FileStore/foodrush/landing/orders/"

# Create the landing zone folder if it doesn't exist
dbutils.fs.mkdirs(raw_landing_path)

print("=" * 55)
print("   FoodRush India — Data Generator")
print("=" * 55)
print(f"  📂 Landing Zone : {raw_landing_path}")
print(f"  ⏱️  Interval     : 10 seconds")
print(f"  📦 Batch size   : 5 records per file")
print("=" * 55)
print()
print("  ▶️  Run the cell below to start generating files.")
print("  ■   Click Interrupt to stop.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📦 Step 2: Full Order Record Pool
# MAGIC
# MAGIC A pool of 40 realistic FoodRush India orders.
# MAGIC Every 10 seconds, 5 are randomly picked and written as a new CSV file.

# COMMAND ----------

import time
import random
from datetime import datetime, timedelta

# CSV header — must match Bronze schema exactly
CSV_HEADER = "order_id,customer_id,restaurant_name,cuisine_type,city,order_date,order_time,food_item,quantity,item_price,delivery_fee,payment_mode,delivery_status,delivery_minutes,customer_rating"

# Pool of 40 order templates
# Fields: restaurant_name, cuisine_type, city, food_item, quantity, item_price, delivery_fee, payment_mode, delivery_status, delivery_minutes, customer_rating
ORDER_POOL = [
    ("Spice Garden",     "South Indian", "Hyderabad", "Masala Dosa",           2, 120.00, 30.00, "UPI",    "Delivered",  35, 4.5),
    ("Biryani House",    "Biryani",      "Hyderabad", "Chicken Biryani",        1, 250.00, 40.00, "Card",   "Delivered",  28, 5.0),
    ("Mumbai Tiffins",   "North Indian", "Mumbai",    "Paneer Butter Masala",   2, 180.00, 35.00, "UPI",    "Delivered",  42, 4.0),
    ("South Spice",      "South Indian", "Bangalore", "Idli Sambar",            3,  80.00, 25.00, "Wallet", "Delivered",  20, 4.8),
    ("Pizza Palace",     "Pizza",        "Pune",      "Margherita Pizza",        1, 320.00, 50.00, "Card",   "Delivered",  55, 3.5),
    ("Dosa Corner",      "South Indian", "Chennai",   "Paper Dosa",              2,  90.00, 20.00, "UPI",    "Delivered",  25, 4.2),
    ("Royal Biryani",    "Biryani",      "Mumbai",    "Mutton Biryani",          1, 350.00, 45.00, "Cash",   "Cancelled",   0, 0.0),
    ("Burger Hub",       "Burger",       "Bangalore", "Veg Burger",              2, 150.00, 30.00, "UPI",    "Delivered",  30, 4.6),
    ("The Veg Kitchen",  "North Indian", "Hyderabad", "Dal Makhani",             1, 160.00, 35.00, "Card",   "Delivered",  38, 4.3),
    ("Noodle Box",       "Chinese",      "Pune",      "Veg Noodles",             2, 130.00, 30.00, "Wallet", "Delivered",  32, 4.7),
    ("Spice Garden",     "South Indian", "Hyderabad", "Uttapam",                 1, 100.00, 30.00, "UPI",    "Delivered",  22, 4.9),
    ("Biryani House",    "Biryani",      "Hyderabad", "Veg Biryani",             2, 200.00, 40.00, "Card",   "Delivered",  29, 4.4),
    ("Mumbai Tiffins",   "North Indian", "Mumbai",    "Chole Bhature",           2, 140.00, 35.00, "UPI",    "Delivered",  48, 3.8),
    ("South Spice",      "South Indian", "Bangalore", "Medu Vada",               4,  60.00, 25.00, "Cash",   "Delivered",  18, 5.0),
    ("Pizza Palace",     "Pizza",        "Pune",      "Farmhouse Pizza",          1, 380.00, 50.00, "Card",   "Cancelled",   0, 0.0),
    ("Dosa Corner",      "South Indian", "Chennai",   "Masala Dosa",             3,  90.00, 20.00, "UPI",    "Delivered",  27, 4.5),
    ("Royal Biryani",    "Biryani",      "Mumbai",    "Chicken Biryani",         2, 260.00, 45.00, "Wallet", "Delivered",  40, 4.1),
    ("Burger Hub",       "Burger",       "Bangalore", "Paneer Burger",            1, 170.00, 30.00, "UPI",    "Delivered",  33, 4.3),
    ("The Veg Kitchen",  "North Indian", "Hyderabad", "Rajma Chawal",            2, 140.00, 35.00, "Card",   "Delivered",  36, 4.6),
    ("Noodle Box",       "Chinese",      "Chennai",   "Fried Rice",              1, 120.00, 20.00, "UPI",    "Delivered",  24, 4.8),
    ("Kebab Express",    "North Indian", "Mumbai",    "Chicken Seekh Kebab",     2, 220.00, 40.00, "Card",   "Delivered",  30, 4.7),
    ("Wrap Zone",        "Fast Food",    "Bangalore", "Paneer Wrap",             1, 130.00, 25.00, "UPI",    "Delivered",  22, 4.5),
    ("The Curry House",  "North Indian", "Hyderabad", "Butter Chicken",          1, 240.00, 35.00, "Wallet", "Delivered",  45, 4.2),
    ("Idli Palace",      "South Indian", "Chennai",   "Mini Idli",               2,  70.00, 20.00, "Cash",   "Delivered",  15, 4.9),
    ("Sandwich Stop",    "Fast Food",    "Pune",      "Grilled Sandwich",        2,  90.00, 20.00, "UPI",    "Delivered",  20, 4.3),
    ("Chaat Wala",       "Street Food",  "Hyderabad", "Pani Puri",               1,  50.00, 15.00, "UPI",    "Delivered",  18, 4.8),
    ("Mughlai Kitchen",  "Mughlai",      "Mumbai",    "Nihari",                  1, 300.00, 50.00, "Card",   "Delivered",  52, 4.0),
    ("Pav Bhaji House",  "Street Food",  "Mumbai",    "Pav Bhaji",               2, 110.00, 25.00, "Cash",   "Delivered",  28, 4.6),
    ("Thali Express",    "South Indian", "Bangalore", "South Indian Thali",      1, 190.00, 30.00, "Wallet", "Delivered",  35, 4.4),
    ("Pasta Point",      "Italian",      "Pune",      "Penne Arrabbiata",        1, 210.00, 40.00, "Card",   "Cancelled",   0, 0.0),
    ("Tandoor Tales",    "North Indian", "Hyderabad", "Paneer Tikka",            2, 200.00, 35.00, "UPI",    "Delivered",  38, 4.7),
    ("Poha House",       "Breakfast",    "Pune",      "Kanda Poha",              2,  65.00, 15.00, "UPI",    "Delivered",  12, 4.9),
    ("Street Biryan",    "Biryani",      "Chennai",   "Ambur Biryani",           1, 180.00, 30.00, "Cash",   "Delivered",  25, 4.8),
    ("Frankie Corner",   "Fast Food",    "Mumbai",    "Paneer Frankie",          2, 100.00, 20.00, "UPI",    "Delivered",  20, 4.5),
    ("Lassi Bar",        "Beverages",    "Hyderabad", "Mango Lassi",             2,  80.00, 15.00, "Wallet", "Delivered",  15, 4.9),
    ("Dhaba Style",      "North Indian", "Bangalore", "Aloo Paratha",            3,  75.00, 20.00, "Cash",   "Delivered",  22, 4.3),
    ("South Bowl",       "South Indian", "Chennai",   "Sambar Sadam",            1, 110.00, 20.00, "UPI",    "Delivered",  18, 4.6),
    ("Healthy Bites",    "Salad",        "Pune",      "Quinoa Salad",            1, 220.00, 35.00, "Card",   "Delivered",  28, 3.9),
    ("Mithai Hub",       "Desserts",     "Mumbai",    "Gulab Jamun",             2,  90.00, 20.00, "UPI",    "Delivered",  16, 5.0),
    ("Rolls Republic",   "Fast Food",    "Bangalore", "Egg Roll",                1, 120.00, 20.00, "Card",   "Cancelled",   0, 0.0),
]

print(f"✅ Order pool ready — {len(ORDER_POOL)} order templates loaded")
print("   Every 10 seconds, 5 random orders will be written as a new CSV file.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Step 3: Start the Generator
# MAGIC
# MAGIC > ▶️ **Run this cell** — it will keep writing files every 10 seconds.
# MAGIC > ■ **Click Interrupt** on this cell when you want to stop.

# COMMAND ----------

# Counters
batch_number  = 1
total_records = 0

# Starting order_id counter — each run generates unique IDs
order_id_counter = 1000

print("🚀 Generator started!")
print("-" * 55)

try:
    while True:
        # Pick 5 random orders from the pool
        selected = random.sample(ORDER_POOL, 5)

        # Current timestamp for order_date and order_time
        now = datetime.now()

        # Build CSV rows
        rows = [CSV_HEADER]
        for template in selected:
            (restaurant_name, cuisine_type, city, food_item,
             quantity, item_price, delivery_fee, payment_mode,
             delivery_status, delivery_minutes, customer_rating) = template

            order_id_counter += 1
            cust_id    = f"CUST{random.randint(200, 999)}"
            order_date = now.strftime("%Y-%m-%d")
            order_time = now.strftime("%H:%M:%S")

            # Cancelled orders have empty delivery_minutes and customer_rating
            if delivery_status == "Cancelled":
                row = (
                    f"ORD{order_id_counter},{cust_id},{restaurant_name},{cuisine_type},"
                    f"{city},{order_date},{order_time},{food_item},{quantity},"
                    f"{item_price:.2f},{delivery_fee:.2f},{payment_mode},{delivery_status},,"
                )
            else:
                row = (
                    f"ORD{order_id_counter},{cust_id},{restaurant_name},{cuisine_type},"
                    f"{city},{order_date},{order_time},{food_item},{quantity},"
                    f"{item_price:.2f},{delivery_fee:.2f},{payment_mode},{delivery_status},"
                    f"{delivery_minutes},{customer_rating}"
                )
            rows.append(row)

        # File name uses timestamp — guarantees uniqueness every batch
        file_timestamp = now.strftime("%Y%m%d_%H%M%S")
        filename       = f"orders_batch_{file_timestamp}.csv"
        file_path      = raw_landing_path + filename
        csv_content    = "\n".join(rows)

        # Write to DBFS landing zone
        dbutils.fs.put(file_path, csv_content, overwrite=True)

        total_records += 5
        print(f"  ✅ Batch {batch_number:>3}  |  {filename}  |  5 records written  |  Total so far: {total_records}")

        batch_number += 1

        # Wait 10 seconds before next batch
        time.sleep(10)

except KeyboardInterrupt:
    print()
    print("-" * 55)
    print(f"🛑 Generator stopped by user.")
    print(f"   Total batches written : {batch_number - 1}")
    print(f"   Total records written : {total_records}")
    print(f"   Files in landing zone : dbfs:/FileStore/foodrush/landing/orders/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📂 Step 4: Verify Files in Landing Zone (Run after stopping)

# COMMAND ----------

print("📂 Files currently in landing zone:")
files = dbutils.fs.ls(raw_landing_path)
print(f"   Total files: {len(files)}")
print()
for f in sorted(files, key=lambda x: x.name):
    print(f"   📄 {f.name}  ({f.size} bytes)")
