import sqlite3
import pandas as pd


#1.Join the tables to list order details

conn = sqlite3.connect('shop_database.db')


cur = conn.cursor()

query = '''
    SELECT 
        o.customer_id,
        o.timestamp,
        o.purchase_revenue,
        oi.number_items,
        oi.purchase_price,
        oi.item_status,
        oi.product_id
    FROM 
        orders o
    JOIN 
        order_items oi
    ON 
        o.customer_id = oi.customer_id AND o.timestamp = oi.timestamp
'''

cur.execute(query)
rows = cur.fetchall()
# pandas DF:

columns = ['customer_id', 'timestamp', 'purchase_revenue', 'number_items', 'purchase_price', 'item_status', 'product_id']
df = pd.DataFrame(rows, columns=columns)

print(df)

conn.close()

#2. Calculate the total revenue and total items sold per customer:
conn = sqlite3.connect('shop_database.db')

cur = conn.cursor()

query = '''
    SELECT 
        o.customer_id,
        SUM(o.purchase_revenue) AS total_revenue,
        SUM(oi.number_items) AS total_items_sold
    FROM 
        orders o
    JOIN 
        order_items oi
    ON 
        o.customer_id = oi.customer_id AND o.timestamp = oi.timestamp
    GROUP BY 
        o.customer_id
    ORDER BY 
        total_revenue DESC
'''

cur.execute(query)
rows = cur.fetchall()


# pandas DF:
columns = ['customer_id', 'total_revenue', 'total_items_sold']
df = pd.DataFrame(rows, columns=columns)


print(df)


conn.close()

#3. Show the running sales by day:
conn = sqlite3.connect('shop_database.db')


cur = conn.cursor()


query = '''
    WITH daily_summary AS (
        SELECT 
            timestamp AS day,
            SUM(CASE WHEN item_status = 'sold' THEN purchase_price * number_items ELSE 0 END) AS daily_sales,
            SUM(CASE WHEN item_status = 'returned' THEN purchase_price * number_items ELSE 0 END) AS daily_returns,
            SUM(CASE WHEN item_status = 'sold' THEN purchase_price * number_items ELSE 0 END) -
            SUM(CASE WHEN item_status = 'returned' THEN purchase_price * number_items ELSE 0 END) AS daily_sales_after_returns
        FROM 
            order_items
        GROUP BY 
            timestamp
    )
    SELECT 
        day,
        daily_sales,
        daily_returns,
        daily_sales_after_returns,
        SUM(daily_sales) OVER (ORDER BY day) AS running_total_sales,
        SUM(daily_returns) OVER (ORDER BY day) AS running_total_returns,
        SUM(daily_sales_after_returns) OVER (ORDER BY day) AS running_total_sales_after_returns
    FROM 
        daily_summary
    ORDER BY 
        day
'''

cur.execute(query)
rows = cur.fetchall()

# pandas DF:
columns = ['day', 'daily_sales', 'daily_returns', 'daily_sales_after_returns', 'running_total_sales', 'running_total_returns', 'running_total_sales_after_returns']
df = pd.DataFrame(rows, columns=columns)

print(df)

conn.close()