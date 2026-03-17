from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row


spark = SparkSession.builder.master('spark://localhost:7077').appName('DataFrame Quickstart').getOrCreate()
df1 = spark.createDataFrame([
    Row(a=1, b='string1', c=date(2026,3,16), d=datetime(2026,3,16,12,51,52,523)),
    Row(a=2, b='string2', c=date(2026,3,16), d=datetime(2026,3,16,12,51,52,524)),
    Row(a=3, b='string3', c=date(2026,3,16), d=datetime(2026,3,16,12,51,52,524))
])
df1.show(truncate=False)
df1.printSchema()

df2 = spark.createDataFrame(data=[
    (1,2.3, 4),(2,3.4,5),(3,4.5,6)
],schema='a long,b string,c long')
df2.show()
df2.printSchema()

# df3 = spark.createDataFrame()

spark.stop()
