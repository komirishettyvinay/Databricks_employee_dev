# Databricks notebook source
dbutils.fs.mount(
	source ='wasbs://bronze@laketest.blob.core.windows.net/',
	mount_point = '/mnt/bronze',
	extra_configs = {'fs.azure.account.key.laketest.blob.core.windows.net':'ObJ5Yy5qU+J6GXIomlfcU5qLuYLLRawGvgqplJL/3EoomfstvYMEUKoHLiTFeR328cbNUFjUjfwC+ASt89ZJLA=='})

# COMMAND ----------

dbutils.fs.mount(
	source ='wasbs://silver@laketest.blob.core.windows.net/',
	mount_point = '/mnt/silver',
	extra_configs = {'fs.azure.account.key.laketest.blob.core.windows.net':'ObJ5Yy5qU+J6GXIomlfcU5qLuYLLRawGvgqplJL/3EoomfstvYMEUKoHLiTFeR328cbNUFjUjfwC+ASt89ZJLA=='})

# COMMAND ----------

dbutils.fs.mount(
	source ='wasbs://gold@laketest.blob.core.windows.net/',
	mount_point = '/mnt/gold',
	extra_configs = {'fs.azure.account.key.laketest.blob.core.windows.net':'ObJ5Yy5qU+J6GXIomlfcU5qLuYLLRawGvgqplJL/3EoomfstvYMEUKoHLiTFeR328cbNUFjUjfwC+ASt89ZJLA=='})

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')
