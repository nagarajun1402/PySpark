from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, \
    StringType, \
    ByteType, ShortType, IntegerType, LongType, DoubleType, FloatType, DecimalType, \
    BinaryType, \
    BooleanType, \
    DataType, TimestampType, DateType
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql.functions import avg, col, format_number, explode, regexp_extract, from_json, try_variant_get, try_parse_json, udf, broadcast, pandas_udf, udtf, arrow_udf, arrow_udtf,  lit
import pandas as pd
import pyarrow as pa
from math import factorial
from json import loads, JSONDecodeError



spark = SparkSession.builder.master('spark://debian:7077').appName('Chapter_4_Bug_Busting_Debugging_PySpark').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
# @udf(returnType='int', useArrow=True)
# def slen(s :str):
#     return len(s)
#
# # spark.conf.set('spark.sql.execution.pythonUDF.arrow.enabled', True)
#
# data = [('naga',), ('gopi',), ('baji',)]
# df1 = spark.createDataFrame(data, ['name'])
# df1.select('name', slen('name')).show()

# @pandas_udf(returnType='string')
# def upper_str(series: pd.Series) -> pd.Series:
#     return series.str.upper()
#
# df2 = df1.select('name', upper_str('name').alias('upper_name'))
# df2.show()

# @arrow_udf(returnType='string')
# def upper_pa(s: pa.Array) -> pa.Array:
#     return pa.compute.ascii_upper(s)
#
# df1.select(upper_pa('name')).show(truncate=False)

# ----------------------------------------------------------------------------------------------------------------------
# data = [
#     ("Hello World", [1, 2, 3]),
#     ("PySpark is Fun", [4, 5, 6]),
#     ("PySpark Rocks", [7, 8, 9])
# ]
# df = spark.createDataFrame(data, ["text_column", "list_column"])
# @udf(returnType='string')
# def process_row(text, numbers):
#     vowels_count = sum(1 for char in text if char in 'aeiouAEIOU')
#     doubled = [x * 2 for x in numbers]
#     return f'Vowels: {vowels_count}, Doubled: {doubled}'
# df.withColumn('process_row', process_row(df['text_column'], df['list_column'])).show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
# data = [
#     (10.0, "Spark"),
#     (20.0, "Big Data"),
#     (30.0, "AI"),
#     (40.0, "Machine Learning"),
#     (50.0, "Deep Learning")
# ]
# df = spark.createDataFrame(data, ["numeric_column", "text_column"])
#
# # Schema for the result
# schema = StructType([
#     StructField("mean_value", DoubleType(), True),
#     StructField("sum_value", DoubleType(), True),
#     StructField("processed_text", MapType(StringType(), ShortType(), True), True),
#     StructField("new_value", ArrayType(ByteType(), True), True)
# ])
# @pandas_udf(schema)
# def compute_mean(numeric_col: pd.Series, text_col: pd.Series) -> pd.DataFrame:
#     full_range = list(range(6))
#     text_val = text_col.map(lambda x: {x: len(x)})
#     pd_df = pd.DataFrame({
#         "mean_value": numeric_col.mean(),
#         'sum_value': numeric_col.sum(),
#         'processed_text': text_val,
#         'new_value': [full_range]
#     })
#     return pd_df
# df.withColumn('compute_mean', compute_mean(df["numeric_column"], df["text_column"])).show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
# @udtf(returnType='num: int, square: int, cube: int, factorial: int')
# class GeneratesComplexNumber():
#     def eval(self, start: int, end: int):
#         for i in range(start, end + 1):
#             yield (i , i ** 2, i ** 3, factorial(i))
# GeneratesComplexNumber(lit(1), lit(6)).show()
# ----------------------------------------------------------------------------------------------------------------------
# @udtf(returnType='word: string, lenght: int, is_palindrome: string')
# class IsPalindrome():
#     def eval(self, word: str):
#         if word == word[::-1]:
#             is_palindrome = True
#         else:
#             is_palindrome = False
#         yield (word, len(word), is_palindrome)
# IsPalindrome(lit('vikatakiv')).show()
# ----------------------------------------------------------------------------------------------------------------------
@udtf(returnType='key: string, value: string, type_val: string')
class ParseJSON():
    def eval(self, json_str: str):
        try:
            json_data = loads(json_str)
            for key, value in json_data.items():
                type_val = type(value).__name__
                yield (key, str(value), type_val)
        except JSONDecodeError:
            print("Invalid JSON")
ParseJSON(lit('{"name": "Alice", "age": 25, "is_student": false}')).show()
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()