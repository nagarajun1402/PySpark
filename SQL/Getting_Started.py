from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.master("spark://localhost:7077").appName("Getting_Started").getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
# df = spark.read.json('../../../spark/examples/src/main/resources/people.json')
# df.show()
# df.printSchema()
# df.select('name').show()
# df.select(df['name'], df['age'] + 1).show()
# df.filter(df['age']>21).show()
# df.groupBy(df['age']).count().show()
# # ----------------------------------------------------------------------------------------------------------------------
# df.createTempView('people')
# sqlDF = spark.sql('select * from people')
# sqlDF.show()
# # ----------------------------------------------------------------------------------------------------------------------
# df.createGlobalTempView('people')
# spark.sql('select * from global_temp.people').show()
# spark.newSession().sql('select * from global_temp.people').show()
# ----------------------------------------------------------------------------------------------------------------------
sc = spark.sparkContext
lines = sc.textFile('../../../spark/examples/src/main/resources/people.txt')
print(lines.collect())
parts = lines.map(lambda l: l.split(","))
print(parts.collect())
# prts = lines.flatMap(lambda l: l.split(","))
# print(prts.collect())
people = parts.map(lambda l: Row(name=l[0], age=l[1].strip()))
# print(people.collect())
# schemaPeople = spark.createDataFrame(people)
# schemaPeople.show()
# schemaPeople.printSchema()
# schemaPeople.createTempView("people")
# spark.sql('select name, age, len(age) from people').show()
# teenagers = spark.sql('select name, age from people where age>=13 and age<=19')
# teenagers.show()
# print(type(teenagers))
# teenNames = teenagers.rdd.map(lambda l: "Name: " + l.name).collect()
# print(teenNames, type(teenNames))
# for name in teenNames:
#     print(name)
# ----------------------------------------------------------------------------------------------------------------------
# people = parts.map(lambda l: (l[0], l[1].strip()))
schemaString = 'name age'
fields = [ StructField(col, StringType(), True) for col in schemaString.split(' ')]
schema = StructType(fields)
print(schema)
schemaPeople = spark.createDataFrame(people, schema)
schemaPeople.printSchema()
schemaPeople.show()
schemaPeople.where(schemaPeople['age'] > 25).show()
# spark.sql('select name, age, len(age) from {schemaPeople} order by name, age', schemaPeople=schemaPeople).show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
