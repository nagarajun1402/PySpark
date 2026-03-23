from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://localhost:7077').appName('CSV Files').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
sc = spark.sparkContext
df1 = spark.read.csv('../../../../spark/examples/src/main/resources/people.csv')
df1.show()

df2 = spark.read.option('delimiter', ';').csv('../../../../spark/examples/src/main/resources/people.csv')
df2.show()

df3 = spark.read.option('delimiter', ';').option('header', True).csv('../../../../spark/examples/src/main/resources/people.csv')
df3.show()

df4 = spark.read.options(delimiter=';', header=True).csv('../../../../spark/examples/src/main/resources/people.csv')
df4.show()

df5 = spark.read.csv('../../../../spark/examples/src/main/resources')
df5.show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
