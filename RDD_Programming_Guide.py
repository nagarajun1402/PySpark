from pyspark import SparkContext, SparkConf
import os, shutil
if os.path.exists('data/output_sequence'):
    shutil.rmtree('data/output_sequence')

conf = SparkConf().setAppName('RDD_Programming_Guide').setMaster('spark://localhost:7077')
sc = SparkContext(conf=conf)

# data = [1, 2, 3, 4, 5]
# distData = sc.parallelize(data, 2)
# print(distData, distData.collect())
# print(distData.reduce(lambda x, y: x + y))
#
# distFile = sc.textFile('data/wordCount.txt')
# print(distFile.collect())
# print(distFile.map(lambda x: len(x)).collect())
# print(distFile.map(lambda x: len(x)).reduce(lambda x, y: x + y))
#
# rdd = sc.parallelize(range(10)).map(lambda x: (x, 'n' * x))
# print(rdd.collect())
# rdd.saveAsSequenceFile('data/output_sequence')
# print(sorted(sc.sequenceFile('data/output_sequence').collect()))
# # ----------------------------------------------------------------------------------------------------------------------
# counter = 0
# rdd = sc.parallelize(data)
#
# def increment_counter(x):
#     global counter
#     counter += x
# rdd.foreach(increment_counter)
# print(counter)
# ----------------------------------------------------------------------------------------------------------------------
lines = sc.textFile('data/wordCount.txt')
pairs = lines.map(lambda x: (x, 1))
counts = pairs.reduceByKey(lambda x, y: x + y)
print(pairs.reduceByKey(lambda x, y: x + y).sortByKey().collect())
print(counts.collect())
# ----------------------------------------------------------------------------------------------------------------------
sc.stop()
