from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, \
    StringType, \
    ByteType, ShortType, IntegerType, LongType, DoubleType, FloatType, DecimalType, \
    BinaryType, \
    BooleanType, \
    DataType, TimestampType, DateType
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql.functions import sum, avg, col, format_number, explode, regexp_extract, from_json, try_variant_get, try_parse_json, lower, udf, lit, sqrt
from pyspark.sql import Row, Column


spark = SparkSession.builder.master('spark://localhost:7077').appName('Chapter_3_Function_Junction_Data_manipulation_with_PySpark').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
# df1 = spark.createDataFrame([
#     Row(age=10, height=80.0, NAME="Alice"),
#     Row(age=10, height=80.0, NAME="Alice"),
#     Row(age=5, height=float("nan"), NAME="BOB"),
#     Row(age=None, height=None, NAME="Tom"),
#     Row(age=None, height=float("nan"), NAME=None),
#     Row(age=9, height=78.9, NAME="josh"),
#     Row(age=18, height=1802.3, NAME="bush"),
#     Row(age=7, height=75.3, NAME="jerry"),
# ])
#
# df1.show()
# df2 = df1.withColumnRenamed('NAME', 'name')
# df2.show()
#
# df3 = df2.na.drop(subset='name')
# df3 = df2.dropna(subset=['name'])
# df3.show()
#
# df4 = df3.na.fill({'age': 10, 'height': 80.1})
# df4 = df3.fillna({'age': 10, 'height': 80.1})
# df4.show()
#
# df5 = df4.where(col('height').between(65, 85))
# df5 = df4.filter(df4['height'].between(65, 85))
# df5 = df4.filter(df4.height.between(65, 85))
# df5 = df4.where(df4.height.between(65, 85))
# df5.show()
#
# df6  = df5.distinct()
# df6.show()
#
# df7 = df6.withColumn('name', lower('name'))
# df7.show()
#
# capitalize = udf(lambda s: s.capitalize())
# df8 = df6.withColumn('name', capitalize('name'))
# df8.show()
#
# df9 = df7.select('name', 'age', 'height')
# df9.show()
# ----------------------------------------------------------------------------------------------------------------------
# df = spark.range(10)
# df.show()
# for i in range(20):
#     df = df.withColumn(f'col_{i}', lit(i))
# df.show()
#
# df2 = df.select('id', 'col_2', 'col_3', sqrt(col('col_4') + col('col_5')).alias('sqrt_col_4_plus_5'))
# df2.show()
#
# df3 = df2.filter(col('id') % 2 == 1)
# df3.show()
# ----------------------------------------------------------------------------------------------------------------------
# df = spark.createDataFrame([
#     Row(incomes=[123.0, 456.0, 789.0], NAME="Alice"),
#     Row(incomes=[234.0, 567.0], NAME="BOB"),
#     Row(incomes=[100.0, 200.0, 100.0], NAME="Tom"),
#     Row(incomes=[79.0, 128.0], NAME="josh"),
#     Row(incomes=[123.0, 145.0, 178.0], NAME="bush"),
#     Row(incomes=[111.0, 187.0, 451.0, 188.0, 199.0], NAME="jerry"),
# ])
# df.show(truncate=False)
#
# df2 = df.select(lower('name').alias('name'), explode('incomes').alias('income'))
# df2.show()
#
# df3 = df2.groupBy('name').agg(avg('income').alias('income_avg'))
# df3.show()
#
# df4 = df3.orderBy('name')
# df4.show()
# ----------------------------------------------------------------------------------------------------------------------
df1 = spark.createDataFrame([
    Row(age=10, height=80.0, name="alice"),
    Row(age=9, height=78.9, name="josh"),
    Row(age=18, height=82.3, name="bush"),
    Row(age=7, height=75.3, name="tom"),
])

df2 = spark.createDataFrame([
    Row(incomes=[123.0, 456.0, 789.0], name="alice"),
    Row(incomes=[234.0, 567.0], name="bob"),
    Row(incomes=[79.0, 128.0], name="josh"),
    Row(incomes=[123.0, 145.0, 178.0], name="bush"),
    Row(incomes=[111.0, 187.0, 451.0, 188.0, 199.0], name="jerry"),
])

df3 = df1.join(df2, on='name', how='left')
df3.show(truncate=False)

df3 = df1.join(df2, on='name', how='right')
df3.show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
