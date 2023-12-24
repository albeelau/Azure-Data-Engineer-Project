# Azure-Data-Engineer-Project

In this project, I will explore the dynamic world of retail analytics with Bike Store Customer Analysis and Sales Trends project. Leveraging a Kaggle competition dataset, this project delves into the diverse and intricate landscape of customer behavior and sales trends within a bike store by using Azure data factory and databricks.

# 1.Why Explore This Project?

-Real-World Relevance: Addressing challenges from a Kaggle competition dataset, this project mirrors the intricacies of real-world business scenarios. 

-End-to-End Solutions: From data ingestion to advanced analytics, witness a complete data engineering and analytics solution. 

-Continuous Learning: Embrace the latest technologies, showcasing a commitment to continuous improvement and staying ahead in the data landscape.


# 2.Project Highlights:

-Dataset Source: Kaggle Competition - Bike Store Customer Analysis. 

-Objective: Analyze customer behavior and identify sales trends to enhance business insights. 

-Technologies Used: Azure Data Factory: Orchestrating the ETL (Extract, Transform, Load) processes efficiently. 

-Data Lake Gen 2: Storing and managing structured and unstructured data securely. 

-Azure Databricks: Leveraging distributed computing for advanced data analytics and machine learning.


# 3.What to Expect:

-Data Ingestion and Preparation: Utilizing Azure Data Factory to ingest data from diverse sources into the scalable Azure Data Lake Gen 2 storage.

-Data Exploration and Transformation: Harnessing the power of Azure Databricks for in-depth data exploration, cleaning, and transformation.

-Customer Segmentation: Applying advanced analytics to segment customers based on purchasing behavior and preferences.

-Sales Trends Analysis: Uncovering actionable insights through trend analysis, forecasting, and anomaly detection.

-Visualizations and Reporting: Presenting findings using compelling visualizations to facilitate data-driven decision-making.


# Data Ingestion and Transformation with Azure Databricks
1. Ingest csv files from Azure Data Lake into Azure Databricks by SAS token
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/36067a9f-4f6e-43bd-a9c9-9f2ddc75f3d9)

2. Read the files on dataframe
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/f5925afb-4c1f-436d-a054-fa15c1e497a7)

3. Rename the columns
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/8492349d-f4f7-402d-8ac9-84c90f531cab)

4. Ingest a new column as "ingestion_date" to DataFrame
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/7e34c3e2-df6a-442c-832f-ae2e19d1575b)

5. Write the result to Datalake as parquet
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/d0225512-7ade-4d48-b7fb-a2a48324c6e1)

# Bike Store Sales and Customer Preferences Analysis - SQL& Python
1. Create database to manage tables or views
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/561d201f-80d0-43ba-812e-96af746b14e6)

2. Access DataFrames using SQL by creating temporary views
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/6d1a4194-395d-428e-975e-23338f2a68d9)

a. Gather insights on category-level sales by calculating total revenue, units sold, and total orders for each product category within one year data(2016):
    
      -"Mountain Bikes" stands out as the top revenue-generating category, contributing the highest revenue of $3,030,775.71.
      -"Cruisers Bicycles" have the highest number of units sold (2,063), indicating strong customer demand in this category.
      -"Electric Bikes" and "Cyclocross Bicycles" have moderate revenue but lower unit sales and total orders. There may be opportunities for targeted marketing or promotions to boost sales in these categories.

 ![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/9cc3790c-29ba-4897-9e68-7ccbfcf78324)

b. Provides seasonal trends from the average of monthly units sold for each product category:
     
      -"Cyclocross Bicycles" and "Electric Bikes," show fluctuations in sales across different months.
      -"Cyclocross Bicycles" and "Comfort Bicycles" become popular among customers throughout the year 2016.
      -December show increased sales for several categories, possibly due to holiday shopping trends.

![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/3eda57a1-a256-4d34-bab6-5a006203b514)

![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/f2dbecb2-ea49-4f88-b1bd-3241beed0570)

c. Analyse customer segmentation by distributing "repeat buyers" and "one-time buyers"/ "recent buyer" and "not recent buyer."/ "big spender," "average spender," and "low spender.
     
     -Targeting strategies can be developed based on the customer segments. For example, specific marketing efforts could be directed towards one-time buyers to encourage repeat purchases.
     -Understanding the characteristics of big spenders and targeting similar customers might be beneficial for increasing overall revenue.
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/c2abf95b-2715-4378-8bff-544d5bd1c02e)
![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/ece742e0-9b21-4515-89a9-0ed3e8e74faf)


d. Shows a mix of customers with varying total transactions and corresponding ranks.
  
   -Highlights both top-performing customers with consistent transaction patterns and instances where customers share the same rank due to equal transaction counts.

![image](https://github.com/albeelau/Azure-Data-Engineer-Project/assets/77976477/c8df78d5-a4c5-4541-8c90-abb769d28744)

# Maintain data pipeline
- Version control and collaboration
- New or modified data incremental load 
- Schedule trigger with ADF
- Validate data

