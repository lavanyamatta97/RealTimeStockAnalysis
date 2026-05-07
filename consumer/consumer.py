from pyspark.sql import SparkSession    
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
from pyspark.sql.functions import from_json, col
import os


#directory where spark will store its checkpoint data. crucial in streaming to enable fault tolerance

checkpoint_dir = "/tmp/checkpoint/kafka_to_postgres"
if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)



#the schema/structure matching the new data coming from kafka. this is needed to parse the json data correctly
kafka_data_schema = StructType([
    StructField("date", StringType()),
    StructField("high", StringType()),
    StructField("low", StringType()),
    StructField("open", StringType()),
    StructField("close", StringType()),
    StructField("symbol", StringType()),
])


spark = (SparkSession.builder
         .appName("KafkaSparkStreaming")
         .getOrCreate())

# here we write code to read batch or stream data from kafka. we specify the kafka server, topic, and other options to control how spark reads the data.
df = ( spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "stock_analysis")
      .option("startingOffsets", "latest") #read only new incoming messages (ignore old messages in the topic)
      .option("failOnDataLoss", "false") #if kafka deletes old messages (retention), spark wont crash
      .load() #start reading the kafka topic as a stream
    
)


#convert the value column (which is a JSON string) into a structured column
#the data we read from the kafka topic is a key-value pair
parsed_df = df.selectExpr('CAST(key as STRING)', 'CAST(value as STRING)') \
                .select(from_json(col("value"), kafka_data_schema).alias("data")) \
                .select("data.*") #flatten the structure to get individual columns      

processed_df = parsed_df.select(
    col("date").cast(TimestampType()).alias("date"),
    col("high").alias("high"),
    col("low").alias("low"),
    col("open").alias("open"),
    col("close").alias("close"),
    col("symbol").alias("symbol")
)


#Display the result to the terminal console output mode

query = (processed_df.writeStream \
         .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", checkpoint_dir) \
            .start())


#wait for the termination of the query
query.awaitTermination()