from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.master('spark://localhost:7077').appName('Parquet Files').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
peopleDF = spark.read.json('../../../../spark/examples/src/main/resources/people.json')
peopleDF.show()
peopleDF.write.mode('overwrite').parquet('../../data/people_df')

parquetFile = spark.read.parquet('../../data/people_df')
parquetFile.show()
parquetFile.createTempView('parquetpeople')
spark.sql('select * from parquetpeople where age between 13 and 19').show()

# spark.sql.sources.partitionColumnTypeInference.enabled
# For these use cases, the automatic type inference can be configured by spark.sql.sources.partitionColumnTypeInference.enabled, which is default to true. When type inference is disabled, string type will be used for the partitioning columns.

# ----------------------------------------------------------------------------------------------------------------------
sc = spark.sparkContext
squareDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda x: Row(single = x, double = x**2)))
squareDF.printSchema()
squareDF.write.mode('overwrite').parquet('../../data/merge_df/key=1')
cubesDF = spark.createDataFrame(sc.parallelize(range(1, 6)).map(lambda x: Row(single = x, triple = x**2)))
cubesDF.printSchema()
cubesDF.write.mode('overwrite').parquet('../../data/merge_df/key=2')
mergedDF = spark.read.option('mergeSchema', 'true').parquet('../../data/merge_df')
mergedDF.printSchema()
mergedDF.show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()