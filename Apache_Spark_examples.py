from pyspark.sql import SparkSession
from pyspark.sql import functions as f


# spark = SparkSession.builder.master('spark://ubuntu:7077').getOrCreate()
spark = SparkSession.builder.master('yarn').getOrCreate()
data = [[1,2,3,45,67]]
cols = ['a','b','c','d','e']
df = spark.createDataFrame(data, cols)
df.printSchema()
df.show()
df = df.withColumnRenamed('a', 'b')
df.printSchema()
df.show()
spark.stop()
