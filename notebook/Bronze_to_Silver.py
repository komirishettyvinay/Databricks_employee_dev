# Databricks notebook source
dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

table_name = []
 
for i in dbutils.fs.ls('mnt/bronze/'):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

for i in table_name:
    path =f'dbfs:/mnt/bronze/{i}/{i}'
    df = spark.read.format('csv').option('inferschema','True').option('header','True').load(path)
    column = df.columns
    print(column)
    df.printSchema()
    
    
    df_null = df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns])
    

    for column_name in column:
        null_count = df_null.select(column_name).collect()[0][0]
        if null_count > 0:
            print(f"Column '{column_name}' has {null_count} null values")
        else:
            print(f"Column '{column_name}' has {null_count} no null values")
    print("\n")


# COMMAND ----------

for i in table_name:
    path = f'dbfs:/mnt/bronze/{i}/{i}'
    
    # Read the CSV file into a DataFrame
    df = spark.read.option('inferSchema', 'True').option('header', 'True').csv(path)

    # Create a new DataFrame with rows containing null values removed
    df_filtered = df.na.drop()

    print(f"Number of rows before removing nulls: {df.count()}")
    print(f"Number of rows after removing nulls: {df_filtered.count()}")

    columns = df_filtered.columns
    print(i)  # Print the table name
    print(columns)  # Print the column names
    df_filtered.printSchema()  # Print the DataFrame schema
    
    # Count the number of null values in the filtered DataFrame
    df_null = df_filtered.select([count(when(col(c).isNull(), c)).alias(c) for c in columns])

    for column_name in columns:
        null_count = df_null.select(column_name).collect()[0][0]
        if null_count > 0:
            print(f"Column '{column_name}' has {null_count} null values")
        else:
            print(f"Column '{column_name}' has {null_count} no null values")
    
    print("\n")


# COMMAND ----------


for i in table_name:
    output_path = f'dbfs:/mnt/silver/{i}/'
    #spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
    df.write.format('delta').option("overwriteSchema", "true").mode("overwrite").save(output_path)


# COMMAND ----------


for i in table_name:
    output_path = f'dbfs:/mnt/silver/{i}/'
    
    # Save the filtered DataFrame as a CSV with null rows removed
    df_filtered.write.format('csv').option('header', 'True').mode('overwrite').save(output_path)

# COMMAND ----------

for i in table_name:
    path = f'dbfs:/mnt/bronze/{i}/{i}'
    
    # Read the CSV file into a DataFrame
    df = spark.read.option('inferSchema', 'True').option('header', 'True').csv(path)

    # Create a new DataFrame with rows containing null values removed
    df_filtered = df.na.drop()

    print(f"Number of rows before removing nulls: {df.count()}")
    print(f"Number of rows after removing nulls: {df_filtered.count()}")

    columns = df_filtered.columns
    print(i)  # Print the table name
    print(columns)  # Print the column names
    df_filtered.printSchema()  # Print the DataFrame schema
    
    # Count the number of null values in the filtered DataFrame
    df_null = df_filtered.select([count(when(col(c).isNull(), c)).alias(c) for c in columns])

    for column_name in columns:
        null_count = df_null.select(column_name).collect()[0][0]
        if null_count > 0:
            print(f"Column '{column_name}' has {null_count} null values")
        else:
            print(f"Column '{column_name}' has {null_count} no null values")
    
    print("\n")
    
    output_path = f'dbfs:/mnt/silver/{i}/'
    
    # Save the filtered DataFrame as a CSV with null rows removed
    df_filtered.write.format('csv').option('header', 'True').mode('overwrite').save(output_path)

