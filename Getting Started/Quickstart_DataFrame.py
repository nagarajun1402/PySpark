from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row, Column
from pyspark.sql.functions import upper, pandas_udf, avg, sum
from pyspark.sql.types import *


spark = SparkSession.builder.master('spark://localhost:7077').appName('Quickstart Dataframe').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
# df1 = spark.createDataFrame([
#     Row(a=1, b=2., name='string1', date=date(2021, 1, 1), datetime=datetime(2021, 1, 1, 12, 13, 14, 123456)),
#     Row(a=1, b=2., name='string1', date=date(2021, 1, 1), datetime=datetime(2021, 1, 1, 12, 13, 14, 456789)),
#     Row(a=1, b=2., name='string1', date=date(2021, 1, 1), datetime=datetime(2021, 1, 1, 12, 13, 14, 789123))
# ])
# df1.show(truncate=False)
# df1.printSchema()

# df2 = spark.createDataFrame([
#     {'a':1, 'b':2., 'name':'string1', 'date':date(2021, 1, 1), 'datetime1':datetime(2021, 1, 1, 12, 13, 14, 123456)},
#     {'a':1, 'b':2., 'name':'string1', 'date':date(2021, 1, 1), 'datetime1':datetime(2021, 1, 1, 12, 13, 14, 456789)},
#     {'a':1, 'b':2., 'name':'string1', 'date':date(2021, 1, 1), 'datetime1':datetime(2021, 1, 1, 12, 13, 14, 789123)},
# ])
# df2.show(truncate=False)
# df2.printSchema()

# df3 = spark.createDataFrame([
#     (1,2.,'naga', date(2025,3,24), datetime(2025,3,24, 12,23,34,123456)),
#     (1,2.,'gopi', date(2025,3,24), datetime(2025,3,24, 12,23,34,123456)),
#     (1,2.,'baji', date(2025,3,24), datetime(2025,3,24, 12,23,34,123456)),
# ], schema=['Id', 'marks', 'name', 'dates', 'datetimes'])
# df3.show(truncate=False)
# df3.printSchema()

# pandas_df = pd.DataFrame({
#     'a': [1, 2, 3],
#     'b': [2., 3., 4.],
#     'c': ['string1', 'string2', 'string3'],
#     'd': [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
#     'e': [datetime(2000, 1, 1, 12, 0), datetime(2000, 1, 2, 12, 0), datetime(2000, 1, 3, 12, 0)]
# })
# df4 = spark.createDataFrame(pandas_df)
# df4.show()
# df4.printSchema()
# df4.show(vertical=True)
# df4.show(1)
# print(df4.columns)
# df4.select('a', 'b', 'c').describe().show()
# print(df4.toPandas().shape)
# print(df4.a)

# print(type(df1.name) == type(upper(df1.name)) == type(df1.name.isNull))
# df1.select(df1.name).show(truncate=False)
# df1.withColumn('upper_name', upper(df1.name)).show(truncate=False)
# df1.filter(df1.a == 1).show(truncate=False)

# @pandas_udf('long')
# def add_one(series: pd.Series) -> pd.Series:
#     return series + 1
# df1.select(add_one(df1.b)).show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------

# df5 = spark.createDataFrame([
#     ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
#     ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
#     ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
# df5.show()
# # df5.groupBy('color').agg(avg('v1').alias('avg_v1'), sum(df5.v1).alias('sum_v1')).show()
# pandas_df = df5.toPandas()
# # print(pandas_df)
# def plus_mean(pandas_df):
#     return pandas_df.assign(v1 = pandas_df.v1 - pandas_df.v1.mean())
# schema = StructType([StructField('color', StringType(), True), StructField('fruit', StringType(), True), StructField('v1', DoubleType(), True), StructField('v2', LongType(), True)])
# df5.groupBy('color').applyInPandas(plus_mean, schema = schema).show()
# print(df5.columns, df5.schema)
# ----------------------------------------------------------------------------------------------------------------------
# df1 = spark.createDataFrame(
#     [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
#     ('time', 'id', 'v1'))
#
# df2 = spark.createDataFrame(
#     [(20000101, 1, 'x'), (20000101, 2, 'y')],
#     ('time', 'id', 'v2'))
#
# def merge_ordered(l, r):
#     return pd.merge_ordered(l, r)
#
# df1.groupby('id').cogroup(df2.groupby('id')).applyInPandas(
#     merge_ordered, schema='time int, id int, v1 double, v2 string').show()
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
spark.stop()