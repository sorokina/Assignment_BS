import sqlite3

# Define the data for the 'orders' table
orders_data = [
    ('cust_1', '2023-01-01', 383.70),
    ('cust_2', '2023-01-08', 112.79),
    ('cust_1', '2023-01-22', 383.30),
    ('cust_1', '2023-01-19', 239.86),
    ('cust_1', '2023-01-03', 148.39),
    ('cust_3', '2023-01-08', 154.04),
    ('cust_2', '2023-01-21', 468.09),
    ('cust_4', '2023-01-31', 70.35),
    ('cust_3', '2023-01-11', 332.59),
    ('cust_2', '2023-01-09', 405.53)
]

# Define the data for the 'order_items' table
order_items_data = [
    ('cust_1', '2023-01-01', 3, 127.90, 'sold', 'belt'),
    ('cust_1', '2023-01-01', 3, 127.90, 'sold', 'hat'),
    ('cust_1', '2023-01-01', 3, 127.90, 'returned', 'shoes'),
    ('cust_2', '2023-01-08', 1, 112.79, 'returned', 'gloves'),
    ('cust_1', '2023-01-22', 3, 127.77, 'returned', 'scarf'),
    ('cust_1', '2023-01-22', 3, 127.77, 'sold', 'jacket'),
    ('cust_1', '2023-01-22', 3, 127.77, 'returned', 'scarf'),
    ('cust_1', '2023-01-19', 5, 47.97, 'sold', 'scarf'),
    ('cust_1', '2023-01-19', 5, 47.97, 'returned', 'hat'),
    ('cust_1', '2023-01-19', 5, 47.97, 'sold', 'hat'),
    ('cust_1', '2023-01-19', 5, 47.97, 'returned', 'jacket'),
    ('cust_1', '2023-01-19', 5, 47.97, 'sold', 'shoes'),
    ('cust_1', '2023-01-03', 1, 148.39, 'sold', 'scarf'),
    ('cust_3', '2023-01-08', 1, 154.04, 'sold', 'shirt'),
    ('cust_2', '2023-01-21', 4, 117.02, 'sold', 'hat'),
    ('cust_2', '2023-01-21', 4, 117.02, 'sold', 'watch'),
    ('cust_2', '2023-01-21', 4, 117.02, 'returned', 'jacket'),
    ('cust_2', '2023-01-21', 4, 117.02, 'sold', 'shoes'),
    ('cust_4', '2023-01-31', 1, 70.35, 'sold', 'belt'),
    ('cust_3', '2023-01-11', 2, 166.30, 'returned', 'watch'),
    ('cust_3', '2023-01-11', 2, 166.30, 'sold', 'shoes'),
    ('cust_2', '2023-01-09', 4, 101.38, 'returned', 'gloves'),
    ('cust_2', '2023-01-09', 4, 101.38, 'sold', 'shoes'),
    ('cust_2', '2023-01-09', 4, 101.38, 'sold', 'scarf'),
    ('cust_2', '2023-01-09', 4, 101.38, 'sold', 'jacket')
]

# Connect to the SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('shop_database.db')

# Create a cursor object to execute SQL commands
cur = conn.cursor()

# Create the 'orders' table
cur.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        customer_id TEXT,
        timestamp TEXT,
        purchase_revenue REAL
    )
''')

# Create the 'order_items' table
cur.execute('''
    CREATE TABLE IF NOT EXISTS order_items (
        customer_id TEXT,
        timestamp TEXT,
        number_items INTEGER,
        purchase_price REAL,
        item_status TEXT,
        product_id TEXT
    )
''')

# Insert data into the 'orders' table
cur.executemany('''
    INSERT INTO orders (customer_id, timestamp, purchase_revenue)
    VALUES (?, ?, ?)
''', orders_data)

# Insert data into the 'order_items' table
cur.executemany('''
    INSERT INTO order_items (customer_id, timestamp, number_items, purchase_price, item_status, product_id)
    VALUES (?, ?, ?, ?, ?, ?)
''', order_items_data)

# Commit the transaction to save changes
conn.commit()

# Close the database connection
conn.close()

print("Data has been successfully imported into the SQLite database.")
