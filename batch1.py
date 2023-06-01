import mysql.connector as connection
import pandas as pd
import findspark
import sys
import csv
import time
findspark.init('/opt/spark/spark-3.4.0-bin-hadoop3/')
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) 
mydb = connection.connect(host="localhost", database = 'reddit',user="root", passwd="new_password",use_pure=True)
query = "Select * from proj;"
df= pd.read_sql(query,mydb)
print(df)
mydb.close() #close the connection
sdf = spark.createDataFrame(df)
sdf.createOrReplaceTempView("Table")
dk=spark.sql('select sum(num) from Table;')
dk.show()
dh=spark.sql('select avg(num) from table;')
dh.show()
dr=spark.sql('select max(num) from table;')
dr.show()
