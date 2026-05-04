// import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating an entry point of this scala script
// singleton scala object containing the main method

object spark_streaming{
  def main (args:Array[String]){

    // instantiating a spark session

    val spark = SparkSession.builder.appName("spark_streaming_consumer").getOrCreate()

    // defining a schema for incoming "transactions" from kafka topic

    val schema = StructType(Array(StructField("transaction_id",StringType,false),
                                  StructField("user_id", StringType,false),
                                  StructField("merchant_id",StringType,false),
                                  StructField("transaction_amount",DoubleType,false),
                                  StructField("transaction_time",StringType,false),
                                  StructField("device_id",StringType,false),
                                  StructField("location",StringType,false),
                                  StructField("is_international",BooleanType,false)
                                  ))

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
    // df.select(col("value").cast("string"))
    

    // but after ".cast("string")" the whole JSON is still in "string" format, spark doesn't know it's meant to be JSON
    // therefore, we need to parse the string into structured columns (using "from_json") & providing the defined schema

   val parsed_df = stream.select(from_json(col("value").cast("string"), schema).alias("data"))

    // this returns the whole string as single column called "data" holding all values
    // we need to flatten it, therefore

   val final_df = parsed_df.select("data.*") 

    // The * unpacks all nested fields into top-level columns:

    // since i am working with streaming data now, unlike static data, where i can use final_df.show()
    // in streaming data, as i ingested data with readStream, i will display data using writeStream
    // this will continue as the data keeps coming in from source, proper streaming pipeline -eh!


    // FRAUD DETECTION PHASE: DETECTING FRAUDULENT HIGH AMOUNT TRANSACTIONS & VELOCTIY FRAUDS
    val highAmountDF = fraud_rules.apply_rules(final_df)
    val velocityDF   = velocity_fraud.velocity_fraud_detection(final_df)
    
    val highAmountTagged = highAmountDF.withColumn("fraud_type", lit("high_amount"))
    val velocityTagged   = velocityDF.withColumn("fraud_type", lit("velocity"))

    // val finalDF = highAmountTagged.unionByName(velocityTagged)

    val highamount_tagged_fraud = highAmountTagged.writeStream.format("console")// display in console for debugging purposes
                                    .outputMode("append") // show new rows
                                    .option("truncate",false) // dont truncate long values
                                    .option("checkpointLocation","/tmp/checkpoints-highamount")
                                    .start()
    val velocity_tagged_fraud = velocityTagged.writeStream.format("console")
                                                          .outputMode("append")
                                                          .option("truncate",false)
                                                          .option("checkpointLocation","/tmp/checkpoints-velocity")
                                                          .start()


    try {
      highamount_tagged_fraud.awaitTermination()
      velocity_tagged_fraud.awaitTermination()  // keep running, don't exit
    }
    catch {
      case e: Exception =>
      println("Stopping stream & closing spark session")
      highamount_tagged_fraud.stop()
      velocity_tagged_fraud.stop()
      spark.stop()
    }
  }
}
      
    

