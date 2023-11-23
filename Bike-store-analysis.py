# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.bikestoredata.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.bikestoredata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.bikestoredata.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-11-23T21:40:58Z&st=2023-11-23T13:40:58Z&spr=https&sig=EJCt8CPnMpMzuQflFC2%2BttJys58pBy95jyhY6sMUvQ0%3D")

# COMMAND ----------


# Read in CSV files
df = spark.read.csv('abfs://bike-store-data@bikestoredata.dfs.core.windows.net/Bike Store Datasets/brands.csv',header = True)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brands
