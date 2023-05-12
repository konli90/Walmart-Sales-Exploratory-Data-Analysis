import os
import pandas as pd
import psycopg2

# Define the path to the directory
path = r'C:\Users\MARYLAND\Documents\GitHub\Walmart-Sales-Exploratory-Data-Analysis'

# Load database credentials from environment variables
host = "localhost"
database = "new_sales_db"
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=host,
    database=database,
    user=uid,
    password=pwd
)

# Define the table name and column names
table_name = "sales"
column_names = ['id','branch','city','customer_type','gender','product_line',
                'unit_price','quantity','tax_5','total','date','time',
                'payment','cogs','gross_margin_percentage','gross_income','rating']

# Read all Excel files in the directory into a single Pandas dataframe
df = pd.concat(pd.read_excel(os.path.join(path, f)) for f in os.listdir(path) if f.endswith('.xlsx'))

# Delete existing data from the table, if any
cur = conn.cursor()
cur.execute(f"SELECT COUNT(*) FROM {table_name}")
row_count = cur.fetchone()[0]
if row_count > 0:
    cur.execute(f"DELETE FROM {table_name}")
    print(f"{row_count} rows have been deleted from {table_name}")

# Insert the data into the table
for index, row in df.iterrows():
    values = [str(row[column_name]) for column_name in column_names]
    cur.execute(f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})", tuple(values))
    conn.commit()

print(f"{len(df)} rows have been added to {table_name}")

# Close the database connection
cur.close()
conn.close()