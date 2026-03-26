from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, MapType, \
    StringType, \
    ByteType, ShortType, IntegerType, LongType, DoubleType, FloatType, DecimalType, \
    BinaryType, \
    BooleanType, \
    DataType, TimestampType, DateType
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql.functions import sum, avg, col, format_number, explode, regexp_extract, from_json, try_variant_get, try_parse_json


spark = SparkSession.builder.master('spark://localhost:7077').appName('Chapter_2_A_Tour_of_PySpark_Data_Types').getOrCreate()
# ----------------------------------------------------------------------------------------------------------------------
schema = StructType([
    StructField('integer_field', IntegerType(), nullable=False),
    StructField('long_field', LongType(), nullable=False),
    StructField('double_field', DoubleType(), nullable=False),
    StructField('float_field', FloatType(), nullable=False),
    StructField('decimal_field', DecimalType(), nullable=False),
    StructField('string_field', StringType(), nullable=False),
    StructField('binary_field', BinaryType(), nullable=False),
    StructField('boolean_field', BooleanType(), nullable=False),
    StructField('date_field', DateType(), nullable=False),
    StructField('timestamp_field', TimestampType(), nullable=False),
])
data = [
    (123, 1234567890123456789, 12345.6789, 123.456, Decimal('12345.67'), "Hello, World!", b'Hello, binary world!', True, date(2020, 1, 1), datetime(2020, 1, 1, 12, 0)),
    (456, 9223372036854775807, 98765.4321, 987.654, Decimal('98765.43'), "Goodbye, World!", b'Goodbye, binary world!', False, date(2025, 12, 31), datetime(2025, 12, 31, 23, 59)),
    (-1, -1234567890123456789, -12345.6789, -123.456, Decimal('-12345.67'), "Negative Values", b'Negative binary!', False, date(1990, 1, 1), datetime(1990, 1, 1, 0, 0)),
    (0, 0, 0.0, 0.0, Decimal('0.00'), "", b'', True, date(2000, 1, 1), datetime(2000, 1, 1, 0, 0))
]
df1 = spark.createDataFrame(data, schema)
df1.show(truncate=False)
# # ----------------------------------------------------------------------------------------------------------------------
schema = StructType([
    StructField("revenue_float", FloatType(), nullable=False),
    StructField("revenue_double", DoubleType(), nullable=False),
    StructField("revenue_decimal", DecimalType(10, 2), nullable=False)
])
data = [
    (12345.67, 12345.6789, Decimal('12345.68')),
    (98765.43, 98765.4321, Decimal('98765.43')),
    (54321.10, 54321.0987, Decimal('54321.10'))
]

df2 = spark.createDataFrame(data, schema=schema)
df2.show()
df2.select(
    format_number(sum(df2['revenue_float']), 2).alias('Total_Revenue_Float'),
    format_number(avg(df2['revenue_float']), 2).alias('Average_Revenue_Float'),
    format_number(sum(df2['revenue_double']), 2).alias('Total_Revenue_Double'),
    format_number(avg(df2['revenue_double']), 2).alias('Average_Revenue_Double'),
    format_number(sum(df2['revenue_decimal']), 2).alias('Total_Revenue_Decimal'),
    format_number(avg(df2['revenue_decimal']), 2).alias('Average_Revenue_Decimal'),
).show()
df2.select(
    format_number(sum(df2['revenue_float']), 3).alias('Total_Revenue_Float'),
    format_number(avg(df2['revenue_float']), 3).alias('Average_Revenue_Float'),
    format_number(sum(df2['revenue_double']), 3).alias('Total_Revenue_Double'),
    format_number(avg(df2['revenue_double']), 3).alias('Average_Revenue_Double'),
    format_number(sum(df2['revenue_decimal']), 3).alias('Total_Revenue_Decimal'),
    format_number(avg(df2['revenue_decimal']), 3).alias('Average_Revenue_Decimal'),
).show()
# ----------------------------------------------------------------------------------------------------------------------
schema = StructType([
    StructField('name', StringType(), True),
    StructField('addresses', ArrayType(StructType([StructField('street', StringType(), True), StructField('city', StringType(), True),StructField('zip', StringType(), True)]), True), True),
    StructField('preferences', MapType(StringType(), StringType(), True), True),
])
data = [
    ('naga4', [('Dalal street', 'Mumbai', '400001'), ('Dalal street', 'Mumbai', '400002'), ('Dalal street', 'Mumbai', '400003')], {'name': 'naga1', 'age': '29'}),
    ('naga5', [('Dalal street', 'Mumbai', '500001'), ('Dalal street', 'Mumbai', '600002'), ('Dalal street', 'Mumbai', '200003')], {'name': 'naga2', 'age': '30'}),
    ('naga6', [('Dalal street', 'Mumbai', '600001'), ('Dalal street', 'Mumbai', '500002'), ('Dalal street', 'Mumbai', '300003')], {'name': 'naga3', 'age': '31'})
]
# spark.createDataFrame(data=data, schema=schema).show(truncate=False)
df3 = spark.createDataFrame(data=data, schema=schema)
df3.select('name', col('addresses')[0], col('preferences')['name']).show(truncate=False)
df3.select('name', explode(col('addresses')), explode(col('preferences'))).show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
schema = StructType([
    StructField("float_column", FloatType(), nullable=True),
    StructField("string_column", StringType(), nullable=True)
])
data = [
    (123.456, "123"),
    (789.012, "456"),
    (None, "789")
]
df4 = spark.createDataFrame(data, schema=schema)
df4.show()
df4.printSchema()
df4 = df4.withColumns({'string_from_float': col('float_column').cast(StringType()), 'integer_from_string': col('string_column').astype(IntegerType())})
df4.printSchema()
# ----------------------------------------------------------------------------------------------------------------------
spark.conf.set('spark.sql.ansi.enabled', False)
schema = StructType([
    StructField('name', StringType(), True)
])
data = [
    [123],
    ['abc'],
    [None]
]
df5 = spark.createDataFrame(data, schema)
df5.show()
df5 = df5.withColumn('casted_col', col('name').astype(IntegerType()))
df5.filter(col('casted_col').isNull() & col('name').isNotNull()).show()
spark.conf.unset('spark.sql.ansi.enabled')
# ----------------------------------------------------------------------------------------------------------------------
df6 = spark.createDataFrame([("100",), ("20x",), ("300",)], ["data"])
valid_df = df6.select('data', regexp_extract(col('data'), '^[0-9]+$', 0))
valid_df.show()

schema = StructType([
    StructField("Employee ID", StringType(), True),
    StructField("Role", ByteType(), True),
    StructField("Location", StringType(), True)
])

df7 = spark.read.csv("../../../spark/examples/src/main/resources/people.csv", schema=schema, sep=';', header=True)
df7.printSchema()
df7.show()
# ----------------------------------------------------------------------------------------------------------------------
json_schema = StructType([
    StructField('name', StringType(), True),
    StructField('age', ByteType(), True),
])
df8 = spark.createDataFrame([
['{"name": "naga", "age": 29}'],
['{"name": "raju", "age": 30}'],
('{"name": "naga", "age": 31}',)
], ['json_str'])
df8.select(from_json(col('json_str'), json_schema,).alias('json')).show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
data = [
    '1234567890123456789',
    '12345.6789',
    '"Hello, World!"',
    'true',
    '{"id": 1, "attributes": {"key1": "value1", "key2": "value2"}}',
    '{"id": 2, "attributes": {"key1": "value3", "key2": "value4"}}',
]

df9 = spark.createDataFrame(data, StringType()).select(try_parse_json(col("value")).alias("variant_data"))
df9.printSchema()
df9.show(truncate=False)

df9.select(
    # try_variant_get(col("variant_data"), "$", "double").alias("double_value"),
    try_variant_get(col("variant_data"), "$", "string").alias("string_value"),
    try_variant_get(col("variant_data"), "$.id", "int").alias("id"),
    try_variant_get(col("variant_data"), "$.attributes.key1", "string").alias("key1"),
    try_variant_get(col("variant_data"), "$.attributes.key2", "string").alias("key2"),
).show(truncate=False)

# Collect data and convert to Python objects
print([row["variant_data"].toPython() for row in df9.collect()])
# ----------------------------------------------------------------------------------------------------------------------
spark.stop()
