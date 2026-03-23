from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://localhost:7077').appName('GenericFileSourceOptions').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
test_corrupt_df0 = spark.read.option('ignoreCorruptFiles', 'true').parquet('../../../../spark/examples/src/main/resources/dir1', '../../../../spark/examples/src/main/resources/dir1/dir2/')
test_corrupt_df0.show()

# spark.sql.files.ignoreMissingFiles for Missing files
spark.sql('set spark.sql.files.ignoreCorruptFiles=true')
test_corrupt_df1 = spark.read.parquet('../../../../spark/examples/src/main/resources/dir1', '../../../../spark/examples/src/main/resources/dir1/dir2/')
test_corrupt_df1.show()

df = spark.read.load('../../../../spark/examples/src/main/resources/dir1', pathGlobs='*.parquet', format='parquet')
df.show()

recursive_load_df = spark.read.format('parquet') \
    .option('ignoreCorruptFiles', 'true') \
    .option('recursiveFileLookup', 'true') \
    .load('../../../../spark/examples/src/main/resources/dir1')
recursive_load_df.show()

# When a timezone option is not provided, the timestamps will be interpreted according to the Spark session timezone (spark.sql.session.timeZone).

df = spark.read.format("parquet") \
    .option("recursiveFileLookup", "true") \
    .option("pathGlobFilter", "*.parquet") \
    .option("modifiedAfter", "2024-01-01T00:00:00") \
    .load('../../../../spark/examples/src/main/resources/dir1')
df.show()

# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
