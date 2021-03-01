from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
import time
import os

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["SPARK_HOME"] = "/usr/local/spark"

spark_sql_kafka = "/usr/local/spark/jars/spark-sql-kafka-0-10_2.11-2.4.7.jar"
kafka_clients = "/usr/local/spark/jars/kafka-clients-2.6.0.jar"


kafka_topic_name = "stockexchange"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to the Kafka!!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    # Create a Spark Session

    spark = SparkSession\
        .builder\
        .appName("spark-kafka2hive")\
        .master("local[*]")\
        .enableHiveSupport()\
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions",2)
    spark.sparkContext.setLogLevel("ERROR")


    """
    kafka topic is read by the Stream DataFrame called df.
    The key & value received by the kafka topic are converted to String 
    then a schema is define for value
    """
    # Dataframe reads topic
    data_df = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers",kafka_bootstrap_servers)\
        .option("subscribe",kafka_topic_name)\
        .option("startingOffsets","latest")\
        .load()

    print("Printing the Schema of the orders_df")
    data_df.printSchema()

    stock_df = data_df.selectExpr("CAST(value AS STRING)","timestamp")


    """
    After the dataframe is read from the topie
    Now needs to define the schema for the dataframe.
    """
    #Define Schema

    stock_schema = StructType()\
        .add("exchange",StringType())\
        .add("index",StringType())\
        .add("isYahooFinance",StringType())\
        .add("longName", StringType()) \
        .add("quoteType", StringType()) \
        .add("score", StringType()) \
        .add("shortname", StringType()) \
        .add("symbol", StringType()) \
        .add("typeDisp", StringType()) \


    """
    Now applying the schema to the DF
    """
    #Applying the schema to the DF

    stock_df1 = stock_df \
        .select(from_json(f.col("value"), stock_schema) \
                .alias("stocks"), "timestamp")

    stock_df2 = stock_df1.select("stocks.*", "timestamp")
    print('Printing the schema of stock_df2:')
    stock_df2.printSchema()

   #  stock_df3 = stock_df2.withWatermark("timestamp","5 seconds").groupBy("open","low","close","volume",'data','timestamp')\
   #      .agg({"close":'avg'})
   #
   #  stock_df4 = stock_df3.select("open", "high","low", "close", "volume", "date", 'timestamp', f.col("avg(close)")) \
   # .alias("rolling_average")

    # stock_df4.printSchema()
    # Write to console
    query = stock_df2 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    query.awaitTermination()

    print("Stream process complete ...")


