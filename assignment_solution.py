import sys
import time
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

from pyspark.sql.types import StructType, StructField, StringType,IntegerType,TimestampType
from pyspark.sql.functions import from_json, col,explode,count,sum,to_json,struct,window
from pyspark.sql.window import Window

import logging

if __name__ == "__main__":

    spark = SparkSession.builder.appName("test").getOrCreate()

    OnlineFeedTopic = "DriverDataLogs"
    KafkaServer = "35.184.206.128:9092"
    KafkaOutputTopic = "OutputDataLog"
    CheckPointLocation = "/tmp/asmita"

    DataFrame = spark.readStream.format("kafka") \
              .option("kafka.bootstrap.servers", KafkaServer) \
              .option("startingOffsets", "earliest") \
              .option("subscribe", OnlineFeedTopic) \
              .load()

    CastData = DataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as driverdatalog")

    UserSchema = StructType() \
                 .add("timestamp", TimestampType()) \
                 .add("driver_id", StringType()) \
                 .add("on_duty", IntegerType())

    StructuredJsonData = CastData.select(from_json("driverdatalog",UserSchema).alias("parsed_value"))

    DataToProcess = StructuredJsonData.selectExpr("parsed_value.timestamp","parsed_value.driver_id","parsed_value.on_duty")

    TotalDriversOnDuty = DataToProcess.withWatermark("timestamp", "1 minutes") \
                                 .groupBy(window("timestamp","1 minutes")) \
                                 .agg(count("driver_id") \
                                 .alias("Total_driver_count"),sum("on_duty") \
                                 .alias("Total_OnDuty_count"),((sum("on_duty")/count("driver_id"))*100) \
                                 .alias("Percnt"))

    LoadOutputToKafka = TotalDriversOnDuty.select(to_json(struct("window","Total_driver_count","Total_OnDuty_count","Percnt")) \
                        .alias("value")).writeStream.format("kafka") \
                        .option("kafka.bootstrap.servers", KafkaServer) \
                        .option("topic", KafkaOutputTopic) \
                        .option("checkpointLocation",CheckPointLocation).outputMode("complete") \
                        .start()


    LoadOutputToKafka.awaitTermination()
