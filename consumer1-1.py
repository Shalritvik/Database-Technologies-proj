from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
import json
import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


consumer = KafkaConsumer('redditstream',
bootstrap_servers=['localhost:9092'],
api_version=(0,11,5),
value_deserializer=lambda m: json.loads(m.decode('ascii')))

# op_file = open("output.json",'w')

# try:
#     for message in consumer:
#             json.dump(message.value['query']['recentchanges'][0],op_file)
#             op_file.write(','+"\n")
# except:
    
 

KAFKA_TOPIC_NAME = 'redditstream'
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
# CHECKPOINT_LOCATION = "LOCAL DIRECTORY LOCATION (FOR DEBUGGING PURPOSES)"
CHECKPOINT_LOCATION = "C:/Users/Shal/Desktop/dbt/kafka"


if __name__ == "__main__":

    # STEP 1 : creating spark session object

    spark = (
        SparkSession.builder.appName("Kafka Pyspark Streaming Learning")
        .master("local[*]")
        .config("spark.ui.port","4050")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # STEP 2 : reading a data stream from a kafka topic

    sampleDataframe = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()

    # STEP 3 : Applying suitable schema
    schema = StructType([
    StructField("title", StringType(), True),
    StructField("body", StringType(), True),
    StructField("author", StringType(), True),
    StructField("created_utc", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("score", StringType(), True)
    ])

    info_dataframe = base_df.select(
        from_json(col("value"),schema).alias("info"), "timestamp"
    )

    info_dataframe.printSchema()
    info_df_fin = info_dataframe.select("info.*", "timestamp")
    info_df_fin.printSchema()

    #q1
    type_df = info_df_fin.groupBy(
        window(info_df_fin.timestamp,"15 minutes"," 15 minutes"),
        info_df_fin.type)
    query_1 = type_df.count()
   

    # query = query.withColumn("query", lit("QUERY3"))
    # result_1 = query_1.selectExpr(
    #     "CAST(avg AS STRING)",
    # ).withColumn("value", to_json(struct("*")).cast("string"),).alias("count")
    result = (
            query_1
            .writeStream
            .outputMode("update")
            .format("console")
            # .option("topic", 'end-topic')
            # .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            # .option("checkpointLocation", CHECKPOINT_LOCATION)
            .start()
            .awaitTermination()
        )
   


    # result.awaitTermination()
