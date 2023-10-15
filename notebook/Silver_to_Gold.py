# Databricks notebook source
dbutils.fs.ls('/mnt/silver')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

table_name = []
 
for i in dbutils.fs.ls('mnt/silver/'):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

for i in table_name:
    path =f'dbfs:/mnt/silver/{i}/'
    df = spark.read.format('csv').option('inferschema','True').option('header','True').load(path)
    column = df.columns
    print(i)
    print(column)
    df.printSchema()
    print("\n")

# COMMAND ----------

"""
 Salary by each department. 
"""

# COMMAND ----------

# Load the 'departments' table
departments_df = spark.read.format('csv').option('inferSchema', 'True').option('header', 'True').load('dbfs:/mnt/silver/departments/')
employees_df = spark.read.format('csv').option('inferSchema', 'True').option('header', 'True').load('dbfs:/mnt/silver/employees/')

# Perform the join operation between the main table (df) and the 'departments' table
df_group = employees_df.join(departments_df, on='department_id', how='left')

# COMMAND ----------

display(df_group)

# COMMAND ----------

# Group by 'department' and calculate the sum of 'salary' for each department
department_salary_sum = df_group.groupBy('department_name').agg({'salary': 'sum'})

# Show the result
display(department_salary_sum)

# COMMAND ----------

"""
Salary by reach region
"""

# COMMAND ----------

locations_df = spark.read.format('csv').option('inferSchema', 'True').option('header', 'True').load('dbfs:/mnt/silver/locations/')
countries_df = spark.read.format('csv').option('inferSchema', 'True').option('header', 'True').load('dbfs:/mnt/silver/countries/')
regions_df = spark.read.format('csv').option('inferSchema', 'True').option('header', 'True').load('dbfs:/mnt/silver/regions/')

# Perform left joins in a single statement
final_df = df_group \
    .join(locations_df, on='location_id', how='left') \
    .join(countries_df, on='country_id', how='left') \
    .join(regions_df, on='region_id', how='left')

# COMMAND ----------

# Remove rows with null values in final_df
final_df_filtered = final_df.na.drop()

display(final_df)

# COMMAND ----------

# Group by 'region_name' and calculate the sum of 'salary' for each region
region_salary_sum = final_df_filtered.groupBy('region_name').agg({'salary': 'sum'})

# Show the result
display(region_salary_sum)

# COMMAND ----------

# Define the target paths in the "gold" layer
gold_region_salary_path = 'dbfs:/mnt/gold/region_salary_sum/'
gold_department_salary_path = 'dbfs:/mnt/gold/department_salary_sum/'

# Save the DataFrames to the "gold" layer in Parquet format
region_salary_sum.write.format('parquet').mode('overwrite').save(gold_region_salary_path)
department_salary_sum.write.format('parquet').mode('overwrite').save(gold_department_salary_path)

