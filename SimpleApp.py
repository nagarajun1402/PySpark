from pyspark.sql import SparkSession
from pyspark.sql import functions as sf


spark = SparkSession.builder.master("spark://localhost:7077").appName("SimpleApp").getOrCreate()

# textFile = spark.read.text('../../spark/README.md')
textFile = spark.read.text('./wordCount.txt')

linesContainsA = textFile.filter(textFile.value.contains('a')).count()
linesContainsB = textFile.filter(textFile.value.contains('b')).count()
print(f'lines Contains, {linesContainsA}, {linesContainsB}')

all_words = textFile.select(sf.explode(sf.split(textFile.value, r'\s+')).alias('words'))
# print(all_words.collect())
wordsContainsA = all_words.filter(sf.col('words').contains('a')).count()
wordsContainsB = all_words.filter(sf.col('words').contains('b')).count()
print(f'words Contains, {wordsContainsA}, {wordsContainsB}')
spark.stop()
