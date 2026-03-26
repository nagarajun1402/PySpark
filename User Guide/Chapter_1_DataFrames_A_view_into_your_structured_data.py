from pyspark.sql import SparkSession

# Define the absolute path to your JAR file
mysql_jar_path = "/home/debian/spark/jars/mysql-connector-j-9.6.0.jar"

spark = SparkSession.builder \
    .master('spark://localhost:7077') \
    .appName('Chapter1') \
    .config('spark.jars', mysql_jar_path) \
    .config('spark.driver.extraClassPath', mysql_jar_path) \
    .getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
# employees = [{"name": "John D.", "age": 30},
#   {"name": "Alice G.", "age": 25},
#   {"name": "Bob T.", "age": 35},
#   {"name": "Eve A.", "age": 28}]
#
# df1 = spark.createDataFrame(employees)
# df1.show()
#
# new_df = df1.select('name')
# new_df.show()

# df2 = spark.read.csv('../../../spark/examples/src/main/resources/people.csv', inferSchema=True, sep=';', header=True)
# df2.show()

# df3 = spark.read.json('../../../spark/examples/src/main/resources/employees.json')
# df3.show()

# url = "jdbc:mysql://localhost:3306/sys"
# table = "host_summary"
# properties = {
#   "user": "root",
#   "password": "root",
#   "driver": "com.mysql.cj.jdbc.Driver"
# }
# df = spark.read.jdbc(url=url, table=table, properties=properties)
# df.show()
# ----------------------------------------------------------------------------------------------------------------------
employees = [{"name": "John D.", "age": 30},
  {"name": "Alice G.", "age": 25},
  {"name": "Bob T.", "age": 35},
  {"name": "Eve A.", "age": 28}]
df = spark.createDataFrame(employees)
df.show(n=2, truncate=False, vertical=True)
df.printSchema()
df.withColumnRenamed("name", "full_name").printSchema()
# ----------------------------------------------------------------------------------------------------------------------
filtered_df = df.filter((df['age'] > 20) & (df['age'] < 30))
filtered_df.show()

where_df = df.where((df['age'] > 20) & (df['age'] < 30))
where_df.show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
