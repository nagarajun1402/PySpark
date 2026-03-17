from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

spark = SparkSession.builder.master("spark://localhost:7077").appName("QuickStart").getOrCreate()
textFile = spark.read.text("../../spark/README.md")

print(textFile.count())
print(textFile.first())

linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
print(linesWithSpark)

print(textFile.select(sf.size(sf.split(textFile.value, r"\s+")).name('numWords')).agg(sf.max(sf.col("numWords")).alias("maxWord")).collect())
wordCounts = textFile.select(sf.explode(sf.split(textFile.value, r'\s+')).name('words')).groupBy('words').count()
print(wordCounts)
print(sorted(wordCounts.collect(),reverse=True,key=len))
print(sorted(wordCounts.collect(),reverse=False,key=len))
linesWithSpark.cache()
print(linesWithSpark)
spark.stop()
