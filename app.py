import pandas as pd
from sqlalchemy import create_engine, text

# EXTRACT
df = pd.read_csv("sales_raw.csv")

#print(df.head())

# TRANSFORM
# Fix capitalization
df["customer_name"] = df["customer_name"].str.title()
df["product"] = df["product"].str.title()

# Total-price
df["total_price"] = df["quantity"] * df["price_per_unit"]

# Change order_date to Datetime
df["order_date"] = pd.to_datetime(df["order_date"])

# Find the weekday name
df["weekday"] = df["order_date"].dt.day_name()

# Connect to MySql
username="root"
password="Root"
host="localhost"
database="elt_practice"

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/")

with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database};"))
    conn.commit()

engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")

# We are going to define our database
with engine.connect() as conn:
    conn.execute(text("""
        create TABLE IF NOT EXISTS sales(
            order_id INT PRIMARY KEY,
            customer_name VARCHAR(255),
            product VARCHAR(255),
            quantity INT,
            price_per_unit FLOAT,
            order_date DATE,
            total_price FLOAT,
            weekday VARCHAR(20));
"""))
    conn.commit()


# LOAD
try:
    df.to_sql("sales", con=engine, if_exists="append", index=False)
except:
    print("Duplicate rows skipped") 

with engine.connect() as conn:
    conn.execute(text("""
        UPDATE sales
        SET total_price = quantity * price_per_unit,
            weekday = DAYNAME(order_date)
        WHERE total_price IS NULL OR weekday IS NULL;                      
"""))
    conn.commit()
    print("Column Updated")

# Generate a report of our data
summary_df= pd.read_sql("""
SELECT product, SUM(total_price) AS total_revenue
FROM sales
GROUP BY product
ORDER By total_revenue DESC;                                                                                             
""", con=engine)

# Export MySql to Excel
full_df = pd.read_sql("SELECT * FROM sales", con=engine)

with pd.ExcelWriter("sales_report.xlsx") as writer:
    full_df.to_excel(writer, sheet_name= "Data", index=False)
    summary_df.to_excel(writer, sheet_name="Summary", index=False)

print("ETL Complete")
