from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://localhost:7077').appName('Text Files').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
sc = spark.sparkContext
df1 = spark.read.text('../../../../spark/examples/src/main/resources/people.txt')
df1.show()

df2 = spark.read.text('../../../../spark/examples/src/main/resources/people.txt', lineSep=',')
df2.show()

df3 = spark.read.text('../../../../spark/examples/src/main/resources/people.txt', wholetext=True)
df3.show()

df1.write.format('text').mode('overwrite').save('../../data/text/text1')
df1.write.mode('overwrite').text('../../data/text/text2', compression='snappy')
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()