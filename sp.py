import findspark
import sys
import csv
import time
findspark.init('/opt/spark/spark-3.4.0-bin-hadoop3/')
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better


df = spark.read.csv('output.csv', header=True, inferSchema=True)
#df.show()

df.createOrReplaceTempView("Table")
df.show()
dy=spark.sql('select * from Table')
dy.show()
ds=spark.sql('select sum(*) from Table')
dk=spark.sql('select avg(*) from Table limit 1;')
dm=spark.sql('select max(score) from Table;')
#ds.show()
data=[]
r=ds.collect()
for i in r:
	data.append([str(e) for e in i])
r=dk.collect()
for i in r:
	data.append([str(e) for e in i])
r=dm.collect()
for i in r:
	data.append([str(e) for e in i])
with open('sol.csv','w',newline='') as file:
	csv_writer=csv.writer(file)
	csv_writer.writerows(data)
time.sleep(5)

