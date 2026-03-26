from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import shutil, os


if os.path.exists('spark-warehouse/somepeople'):
    shutil.rmtree('spark-warehouse/somepeople')

spark = SparkSession.builder.master('spark://localhost:7077').getOrCreate()
# spark = SparkSession.builder.master('yarn').getOrCreate()
data = [("sue", 32),
        ("li", 3),
        ("bob", 75),
        ("heo", 13), ]
cols = ["first_name", "age"]
df = spark.createDataFrame(data, cols)
df.show()

df1 = df.withColumn('life_stage', when(col('age') < 13, "Child")
                    .when(col('age').isin(13, 19), "Teenager")
                    .otherwise("Adult"))
df1.show()

df1.where(col('life_stage').isin(['Teenager', 'Adult'])).show()

df1.select(avg('age').alias('avg_age')).show()
df1.groupBy('life_stage').agg(avg('age').alias('avg_AGE')).show()

spark.sql("select avg(age) as avg_age from {df1}", df1=df1).show()
spark.sql('select life_stage, avg(age) as avg_AGE from {df1} group by life_stage', df1=df1).show()

spark.sql("DROP TABLE IF EXISTS somepeople")
df1.write.saveAsTable('somepeople', mode='overwrite')
spark.sql('select * from somepeople').show()
spark.sql("insert into somepeople values ('frank', 4, 'Child')")
spark.sql('select * from somepeople where life_stage="Child"').show()

spark.stop()
