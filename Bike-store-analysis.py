# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Ingest csv files from Azure Data Lake into Azure Databricks by SAS token

# COMMAND ----------

# Access the data with SAS token
spark.conf.set("fs.azure.account.auth.type.bikestoredata.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.bikestoredata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.bikestoredata.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-23T21:40:58Z&st=2023-11-23T13:40:58Z&spr=https&sig=EJCt8CPnMpMzuQflFC2%2BttJys58pBy95jyhY6sMUvQ0%3D")

# COMMAND ----------

# Specify your storage account name and key
storage_account_name = "bikestoredata"
storage_account_key = "m6ZfkINTcnmbeF3Ev6ci4S84poqj5huiq01CYA4RQnkq3QXm4ueLkQcUy+G7Hd/HoRIqZBoM9zNM+AStJLXe8A=="

# Specify the container and mount point
container_name = "bike-store-data"
mount_point = "/mnt/bikestoredata/bike-store-data"

# Mount the storage
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
  mount_point = mount_point,
  extra_configs = {"fs.azure.account.key." +storage_account_name + ".blob.core.windows.net": storage_account_key}
)

# COMMAND ----------

# Find the path
display(dbutils.fs.ls("/mnt/bikestoredata/bike-store-data"))

# COMMAND ----------

# Read the files on dataframe
brands_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/brands.csv",header = True)
categories_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/categories.csv",header = True)
customers_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/customers.csv",header = True)
orders_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/orders.csv",header = True)
order_items_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/order_items.csv",header = True)
products_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/products.csv",header = True)
staffs_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/staffs.csv",header = True)
stocks_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/stocks.csv",header = True)
stores_df = spark.read.csv("dbfs:/mnt/bikestoredata/bike-store-data/Bike Store Datasets/Bike Store Datasets/stores.csv",header = True)

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Access dataframes using SQL by creating temporary views 

# COMMAND ----------

# Access dataframes using SQL by creating temporary views 

brands_df.createTempView("v_brands") 
categories_df.createTempView("v_categories") 
customers_df.createTempView("v_customers") 
orders_df.createTempView("v_orders") 
order_items_df.createTempView("v_order_items") 
products_df.createTempView("v_products") 
staffs_df.createTempView("v_staffs") 
stocks_df.createTempView("v_stocks") 
stores_df.createTempView("v_stores") 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_orders

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####3. Create database to manage tables or views 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN demo

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Bike Store Sales and Customer Preferences Analysis - SQL& Python

# COMMAND ----------

# MAGIC %md
# MAGIC ###### a. Gather insights on category-level sales by calculating total revenue, units sold, and total orders for each product category within one year data(2016):  
# MAGIC           -"Mountain Bikes" stands out as the top revenue-generating category, contributing the highest revenue of $3,030,775.71.
# MAGIC           -"Cruisers Bicycles" have the highest number of units sold (2,063), indicating strong customer demand in this category.
# MAGIC           -"Electric Bikes" and "Cyclocross Bicycles" have moderate revenue but lower unit sales and total orders. There may be opportunities for targeted marketing or promotions to boost sales in these categories.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW re_category_sales 
# MAGIC AS 
# MAGIC WITH category_sales AS (
# MAGIC     SELECT
# MAGIC         DISTINCT
# MAGIC         order_id,
# MAGIC         v_order_items.product_id,
# MAGIC         quantity,
# MAGIC         v_order_items.list_price,
# MAGIC         quantity * v_order_items.list_price AS line_subtotal,
# MAGIC         category_id
# MAGIC     FROM
# MAGIC         v_order_items
# MAGIC     INNER JOIN
# MAGIC         v_products
# MAGIC     ON
# MAGIC         v_order_items.product_id = v_products.product_id
# MAGIC         
# MAGIC )
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC      v_categories.category_name AS category_name,
# MAGIC     ROUND(SUM(line_subtotal),2) AS revenue,
# MAGIC     SUM(quantity) AS units_sold,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders
# MAGIC FROM 
# MAGIC     category_sales
# MAGIC LEFT JOIN
# MAGIC     v_categories
# MAGIC ON 
# MAGIC     v_categories.category_id = category_sales.category_id
# MAGIC GROUP BY
# MAGIC     v_categories.category_name
# MAGIC ORDER BY
# MAGIC     revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### b. Provides seasonal trends from the average of monthly units sold for each product category:
# MAGIC           -"Cyclocross Bicycles" and "Electric Bikes," show fluctuations in sales across different months.
# MAGIC           -"Cyclocross Bicycles" and "Comfort Bicycles" become popular among customers throughout the year 2016.
# MAGIC           -December show increased sales for several categories, possibly due to holiday shopping trends.

# COMMAND ----------


import matplotlib.pyplot as plt ;
import seaborn as sns

# COMMAND ----------

query =  """
WITH product_sales AS (
    SELECT
        EXTRACT(YEAR FROM order_date) AS year,
EXTRACT(MONTH FROM order_date) AS month,
        product_id,
        SUM(quantity) AS units_sold
    FROM
        v_orders
    LEFT JOIN
        v_order_items
    ON
        v_orders.order_id = v_order_items.order_id
    GROUP BY
        1,2,3
),

 product_categories AS (
    SELECT
        product_id,
        category_name
    FROM
        v_products
    LEFT JOIN
        v_categories
    ON
        v_products.category_id = v_categories.category_id
)

SELECT
    month,
    category_name,
    ROUND(AVG(units_sold),0) AS avg_units_sold
FROM
    product_sales
LEFT JOIN
    product_categories
ON
    product_sales.product_id = product_categories.product_id
GROUP BY
    1,2;
 """

result_df = spark.sql(query)

display(result_df)

# Create a bar plot using Seaborn and Matplotlib
fig, ax = plt.subplots(figsize=(9, 5))
sns.barplot(data=result_df.toPandas(), x='month', y='avg_units_sold', hue='category_name', ax=ax)
ax.set_ylabel('Average Units Sold')
ax.set_xlabel('Month')
ax.set_title('Average Units Sold by Month of Year')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### c. Analyse customer segmentation by distributing "repeat buyers" and "one-time buyers"/ "recent buyer" and "not recent buyer."/ "big spender," "average spender," and "low spender.
# MAGIC          -Targeting strategies can be developed based on the customer segments. For example, specific marketing efforts could be directed towards one-time buyers to encourage repeat purchases.
# MAGIC          -Understanding the characteristics of big spenders and targeting similar customers might be beneficial for increasing overall revenue.

# COMMAND ----------

query2 = """
WITH customer_stats AS (
    SELECT
        customer_id,
        SUM(quantity * list_price * (1 - discount)) AS total_spent,
        COUNT(DISTINCT v_orders.order_id) AS total_orders,
       DATEDIFF('2018-12-29', MAX(order_date)) AS days_since_last_purchase
    FROM
        v_orders
    INNER JOIN
        v_order_items
    ON
        v_orders.order_id = v_order_items.order_id
    GROUP BY
        1
)

SELECT
    customer_id,
    CASE WHEN total_orders > 1 THEN 'repeat buyer'
         ELSE 'one-time buyer'
         END AS purchase_frequency,
    CASE WHEN days_since_last_purchase < 90 THEN 'recent buyer'
         ELSE 'not recent buyer'
         END AS purchase_recency,
    CASE WHEN total_spent/(SELECT MAX(total_spent) FROM customer_stats) >= .65 THEN 'big spender'
         WHEN total_spent/(SELECT MAX(total_spent) FROM customer_stats) <= .30 THEN 'low spender'
         ELSE 'average spender' 
         END AS buying_power
FROM
    customer_stats
"""
result_df2 = spark.sql(query2)

# Display the DataFrame
display(result_df2)

fig, ax = plt.subplots(ncols=3, figsize=(16, 4), sharey=True)
sns.countplot(data=result_df2.toPandas(), x='purchase_frequency', ax=ax[0])
sns.countplot(data=result_df2.toPandas(), x='purchase_recency', ax=ax[1])
sns.countplot(data=result_df2.toPandas(), x='buying_power', ax=ax[2])
ax[0].set_title('Purchase Frequency')
ax[1].set_title('Purchase Recency')
ax[2].set_title('Buying Power')
for axis in ax:
    axis.set_ylabel('')
    axis.set_xlabel('')
plt.show()




# COMMAND ----------

# MAGIC %md
# MAGIC ###### d. Shows a mix of customers with varying total transactions and corresponding ranks.
# MAGIC        -Highlights both top-performing customers with consistent transaction patterns and instances where customers share the same rank due to equal transaction counts.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Selecting customer name, total transactions, and rank based on the number of transactions
# MAGIC SELECT
# MAGIC     c.first_name || ' ' || c.last_name AS customer_name,
# MAGIC     COUNT(s.order_id) AS total_transactions,
# MAGIC     RANK() OVER (ORDER BY COUNT(s.order_id) DESC) AS rank
# MAGIC FROM
# MAGIC     v_orders s
# MAGIC LEFT JOIN
# MAGIC     v_customers c
# MAGIC ON s.customer_id = c.customer_id
# MAGIC GROUP BY
# MAGIC     1
# MAGIC ORDER BY
# MAGIC     2 DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####5. Implement Full Load and Incremental Load
# MAGIC

# COMMAND ----------


