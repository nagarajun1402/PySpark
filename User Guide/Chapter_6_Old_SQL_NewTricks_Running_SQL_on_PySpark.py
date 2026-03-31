from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, \
    StringType, \
    ByteType, ShortType, IntegerType, LongType, DoubleType, FloatType, DecimalType, \
    BinaryType, \
    BooleanType, \
    DataType, TimestampType, DateType
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql.functions import avg, col, format_number, explode, regexp_extract, from_json, try_variant_get, try_parse_json, udf, broadcast, pandas_udf, udtf, arrow_udf, arrow_udtf,  lit, sum, rank
import pandas as pd
import pyarrow as pa
from math import factorial
from json import loads, JSONDecodeError
import shutil, os
from pyspark.sql.window import Window
import warnings
warnings.filterwarnings("ignore")

paths_to_clean = ['../spark-warehouse/people', '../spark-warehouse/orders', '../spark-warehouse/people2']
for path in paths_to_clean:
    if os.path.exists(path):
        shutil.rmtree(path)

spark = SparkSession.builder.master('spark://debian:7077').config('spark.sql.warehouse.dir', '../spark-warehouse').appName('Chapter_6_Old_SQL_NewTricks_Running_SQL_on_PySpark').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# ----------------------------------------------------------------------------------------------------------------------
# spark.sql("DROP TABLE IF EXISTS people")
spark.sql("""
CREATE TABLE people USING PARQUET
AS SELECT * FROM VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30) t(id, name, age)
""")
#
# spark.sql("SELECT name, age FROM people WHERE age > 21").show()
#
df1 = spark.read.table("people")
#
# df1.select("name", "age").filter("age > 21").show()
#
# spark.sql('select name from people where age > 21').show()
# spark.read.table('people').select('name').where('age > 21').show()
#
# spark.sql("DROP TABLE IF EXISTS orders")
spark.sql("""
CREATE TABLE orders USING PARQUET
AS SELECT * FROM VALUES (101, 1, 200), (102, 2, 150), (103,3, 300) t(order_id, customer_id, amount)
""")
#
df2 = spark.read.table('orders')
#
# spark.sql('select name, order_id from people p left join orders o on p.id=o.customer_id').show()
# df1.join(df2, df1.id == df2.customer_id, how='left').show()
#
# spark.sql('select name, sum(amount) as total_amount from people p left join orders o on p.id=o.customer_id group by name').show()
# df1.join(df2, df1.id == df2.customer_id).groupBy('name').agg(sum('amount').alias('total_amount')).show()
#
# spark.sql('select name, amount, rank() over(partition by name order by amount desc) as rank from people p join orders o on p.id=o.customer_id').show()
# window_spec = Window.partitionBy('name').orderBy(df2.amount.desc())
# df1.join(df2, df1.id == df2.customer_id).withColumn('rank', rank().over(window_spec)).show()
#
spark.sql("CREATE OR REPLACE TEMP VIEW people2 AS SELECT * FROM VALUES (1, 'Alice', 10), (4, 'David', 35) t(id, name, age)")
# spark.sql('select * from people union select * from people2').show()
df3 = spark.read.table('people2')
# df1.union(df3).show()
# df1.union(df3).dropDuplicates().show()

# print(spark.conf.get('spark.sql.shuffle.partitions'))
# spark.sql('set spark.sql.shuffle.partitions=10').show()
# spark.sql('set spark.sql.shuffle.partitions').show(truncate=False)
#
# spark.conf.set('spark.sql.shuffle.partitions', 200)
# print(spark.conf.get('spark.sql.shuffle.partitions'))

# spark.sql('show tables').show()
# tables = spark.catalog.listTables()
# for table in tables:
#     print(table, table.isTemporary)

# df1.show()
# df1.withColumn('new_col', df1['age'] + 10).show()
# df1.withColumn('age', df1['age'] + 10).show()
#
# spark.sql('select * from people').filter(df1.age > 21).show(truncate=False)
#
# df1.selectExpr('age', 'age + 1 as age_plus_one').show(truncate=False)

df1.createTempView('people')
# spark.sql('select * from people').show()

@udf(returnType=StringType())
def upper_name(s: str):
    return s.upper()

spark.udf.register('upper_naga', upper_name)
spark.sql('select name, upper_naga(name) as upper_nm from people').show()

# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
