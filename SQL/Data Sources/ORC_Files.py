from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://localhost:7077').appName('ORC Files').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
sc = spark.sparkContext

# peopleDF = spark.read.json('../../../../spark/examples/src/main/resources/people.json')
# peopleDF.printSchema()
# peopleDF.show()
# peopleDF.createTempView('people')
# spark.sql('select * from people where age between 13 and 19').show()
jsonStrings = ['{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}']
otherPeopleRDD = sc.parallelize(jsonStrings)
otherPeopleDF = spark.read.json(otherPeopleRDD)
otherPeopleDF.printSchema()
otherPeopleDF.show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()