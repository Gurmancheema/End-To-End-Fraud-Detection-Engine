// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

// creating an entry point of this scala script
// singleton scala object containing the main method

object ingestion_spark_streaming{
  def main (args:Array[String]){

    // instantiating a spark session

    val spark = SparkSession.builder.appName("kafka_to_bronze").getOrCreate()

    // read stream from kafka

    val stream = spark.readStream.format("kafka")
                                 .option("kafka.bootstrap.servers","localhost:9092")
                                 .option("subscribe","transactions")
                                 .option("startingoffsets","latest")
                                 .option("checkpointLocation", "/tmp/checkpoints-fraud")
                                 .load()

    // this returns a spark dataframe, with columns such as "key","value","topic","partition","offset","timestamp"
    // so dataframe "stream" here is just raw bytes, spark has no idea what's inside "value"
    // so ideally, next step is converting the raw bytes into a string, therefore:

    val raw_data_stream = stream.select(col("value").cast("string"))
                            .withColumn("ingestion_time",current_timestamp())
    
    // persisting this raw data to bronze storage layer
    
    val bronze_layer_path = "/home/gurman/End-To-End-Fraud-Detection-Engine/data/raw_bronze_layer"
    val writing_to_bronze_layer = raw_data_stream.writeStream.format("parquet")
                               .option("path",bronze_layer_path)
                               .option("checkpointLocation","/tmp/checkpoints-raw_bronze_layer_parquet")
                               .outputMode("append")
                               .start()

    val writing_to_console = raw_data_stream.writeStream.format("console")
                                            .option("checkpointLocation","/tmp/checkpoints-raw_bronze_layer")
                                            .outputMode("append")
                                            .start()
    try {
      writing_to_bronze_layer.awaitTermination()
      writing_to_console.awaitTermination() // keep running, don't exit
    }
    catch {
      case e: Exception =>
      println("Stopping stream & closing spark session")
      writing_to_bronze_layer.stop()
      writing_to_console.stop()
      spark.stop()
    }
  }
}
      
    

