from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://localhost:7077').getOrCreate()
txt_file = spark.sparkContext.textFile('file:///home/ubuntu/PycharmProjects/PySpark/wordCount.txt')
counts = txt_file.flatMap(lambda x: x.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
print(counts, type(counts))
print(sorted(counts.collect()), type(counts.collect()))
spark.stop()
