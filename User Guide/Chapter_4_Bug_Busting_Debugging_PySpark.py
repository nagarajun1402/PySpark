from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, \
    StringType, \
    ByteType, ShortType, IntegerType, LongType, DoubleType, FloatType, DecimalType, \
    BinaryType, \
    BooleanType, \
    DataType, TimestampType, DateType
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql import functions as f
from pyspark import storagelevel


spark = SparkSession.builder.master('spark://debian:7077') \
    .config('spark.executor.memory', '4G') \
    .config('spark.driver.memory', '1G') \
    .config('spark.executor.cores', '4') \
    .appName('Chapter_4_Bug_Busting_Debugging_PySpark').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
# @udf('integer')
# def my_udf(x):
#     print('What is going on?')
#     return x
#
# spark.range(5).select(my_udf(col('id'))).collect()
# # ----------------------------------------------------------------------------------------------------------------------
# df1 = spark.createDataFrame([(x,) for x in range(100)])
# df2 = spark.createDataFrame([(x,) for x in range(10)])
# df1.join(broadcast(df2), '_1').explain()
# df1.join(df2, '_1').explain()
# df1.join(df2, how='left').explain()
# df1.join(broadcast(df2), '_1').collect()
# df1.join(df2, '_1').collect()
# df1.join(df2, how='left').collect()
# ----------------------------------------------------------------------------------------------------------------------

df1 = spark.read.parquet('../data/yellow_tripdata_2025-02.parquet')
df2 = spark.read.parquet('../data/yellow_tripdata_2026-02.parquet')

df1 = df1.select('VendorID', 'tpep_pickup_datetime','fare_amount')
df2 = df2.select('VendorID', 'tpep_pickup_datetime','fare_amount')

df1 = df1.withColumn('pickup_date', f.to_date(df1['tpep_pickup_datetime']))
df2 = df2.withColumn('pickup_date', f.to_date(df2['tpep_pickup_datetime']))

df1.printSchema()
df2.printSchema()

# df1.groupBy(['VendorID', 'pickup_date']).sum('fare_amount').show()
# df2.groupBy(['VendorID', 'pickup_date']).sum('fare_amount').show()

# df1.groupBy('pickup_date').sum('fare_amount').show()
# df1.groupBy('pickup_date').agg({'fare_amount': f.sum, 'fare_amount': f.avg}).show()

# df2.groupBy('pickup_date').sum('fare_amount').show()
df2.groupBy('pickup_date').agg(f.sum('fare_amount').try_cast('decimal(38,2)').alias('sum_amount'), f.avg('fare_amount').try_cast('decimal(38,2)').alias('avg_amount')).orderBy('pickup_date').show()
# df1.show()
# df2.show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()