from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://localhost:7077').appName('DataSource').getOrCreate()
# the default data source (parquet unless otherwise configured by spark.sql.sources.default) will be used for all operations.
# ----------------------------------------------------------------------------------------------------------------------
# users_df = spark.read.load('../../../spark/examples/src/main/resources/users.parquet')
# users_df.show()
# users_df.select('name', 'favorite_color').write.mode('overwrite').save('../data/users_parquet')
# spark.sql('select * from parquet.`../data/users_parquet`').show()

# people_df = spark.read.load('../../../spark/examples/src/main/resources/people.json', format='json')
# people_df.show()
# people_df.select('age','name').write.format('json').mode('overwrite').save('../data/people_json')
# spark.sql('select * from json.`../data/people_json`').show()

# people_df = spark.read.load('../../../spark/examples/src/main/resources/people.csv', inferSchema=True, header=True, sep=';', format='csv')
# people_df.printSchema()
# people_df.show()
# people_df.write.mode('overwrite').format('csv').save('../data/people_csv')
# spark.sql("SELECT * FROM csv.`../data/people_csv`").show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
