// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method

object silver_gold_streaming{
  def main(args:Array[String]) {

    // instantiating a spark session
    val spark = SparkSession.builder().appName("silver_to_gold_streaming").getOrCreate()

    // defining a schema to read the transformed dataframe from the silver layer

    val schema = StructType(Array(StructField("transaction_id",StringType,false),
                                  StructField("user_id", StringType,false),
                                  StructField("merchant_id",StringType,false),
                                  StructField("transaction_amount",DoubleType,false),
                                  StructField("transaction_time",StringType,false),
                                  StructField("device_id",StringType,false),
                                  StructField("location",StringType,false),
                                  StructField("is_international",BooleanType,false),
                                  StructField("ingestion_time",TimestampType,false)
                                  ))
    

    val incoming_stream_from_silver = spark.readStream.format("parquet")
                                                      .schema(schema)
                                                      .load("../data/transformed_silver_layer")

    // sinking this read dataframe to console to verify before applying transformations

   // val sinking_stream_to_console = incoming_stream_from_bronze.writeStream
     //                                                          .format("console")
       //                                                        .outputMode("append")
         //                                                      .option("truncate",false)
           //                                                    .option("checkpointLocation","/tmp/checkpoints-sink")
             //                                                  .start()

    // ************************* APPLYING FRAUD RULES TO INCOMING TRANSACTIONAL DATA STREAM ***************************
    val highAmountDF = fraud_rules.apply_rules(incoming_stream_from_silver)
    val velocityDF   = velocity_fraud.velocity_fraud_detection(incoming_stream_from_silver)
    
    val highAmountTagged = highAmountDF.withColumn("fraud_type", lit("high_amount"))
    val velocityTagged   = velocityDF.withColumn("fraud_type", lit("velocity"))

    // saving the fraudulent transactions in the gold/fraud layer

    val highamount_fraud_transactions = highAmountTagged.writeStream
                                                      .format("parquet")
                                                      .option("path","../data/gold_layer/high_amount_fraud")
                                                      .outputMode("append")
                                                      .option("checkpointLocation","/tmp/checkpoints-highamount_parquet").start()

    val velocity_fraud_transactions = velocityTagged.writeStream
                                                  .format("parquet")
                                                  .option("path","../data/gold_layer/velocity_fraud")
                                                  .outputMode("append")
                                                  .option("checkpointLocation","/tmp/checkpoints-velocity_parquet")
                                                  .start()
    // val finalDF = highAmountTagged.unionByName(velocityTagged)

    // display in console for debugging purposes

    val highamount_tagged_fraud = highAmountTagged.writeStream.format("console")
                                    .outputMode("append") // show new rows
                                    .option("truncate",false) // dont truncate long values
                                    .option("checkpointLocation","/tmp/checkpoints-highamount")
                                    .start()

    val velocity_tagged_fraud = velocityTagged.writeStream.format("console")
                                                          .outputMode("update")
                                                          .option("truncate",false)
                                                          .option("checkpointLocation","/tmp/checkpoints-velocity")
                                                          .start()

  // since both the fraud rules are sinking properrly now, it iss time to build a unified fraud sink
  // all transactions which are fraudulent by either of the rules shall be in same dataframe
  // To achieve this, first step is to match the schemas of both the dataframes

  // STEP 1: Expand Velocity fraud to transaction level
  // since the velocity fraud's schema currently shows abstract schema
  // therefore, joining it back to transformed silver layer dataset to get more information on fraudulent transacs.

  val velocityExpandeddf = velocityTagged.withColumn("start",col("window.start"))
                                         .withColumn("end",col("windows.end"))
                                         .drop(col("window"))
                                         .join(incoming_stream_from_silver,
                                               col("user_id") === incoming_stream_from_silver("user_id") &&
            incoming_stream_from_silver("transaction_time").between(col("start"),col("end")))

  // sinking this dataframe to console for debugging purposes

  val sinking_VelocityExpandeddf = velocityExpandeddf.writeStream.format("console")
                                                     .outputMode("update")
                                                     .option("checkpointLocations","/tmp/checkpoints-expandedvelocity")
                                                     .start()


  // STEP 2: Nomalizing the schema for velocityExpandeddf

  val cleaned_velocity_df = velocityExpandeddf.withColumn("triggered_rule",lit("HIGH_VELOCITY"))
                                              .select("transaction_id",
                                                      "user_id",
                                                      "merchant_id",
                                                      "transaction_amount",
                                                      "transaction_time",
                                                      "device_id",
                                                      "location",
                                                      "is_international",
                                                      "ingestion_time",
                                                      "triggered_rule"
                                                      )


    try {
       // sinking_stream_to_console.awaitTermination()
       highamount_fraud_transactions.awaitTermination()
       velocity_fraud_transactions.awaitTermination()
       
       highamount_tagged_fraud.awaitTermination()
       velocity_tagged_fraud.awaitTermination()

       sinking_VelocityExpandeddf.awaitTermination()
    }
    catch {
      case e: Exception =>
        println("stopping write stream and spark session")
        highamount_fraud_transactions.stop()
        velocity_fraud_transactions.stop()

        highamount_tagged_fraud.stop()
        velocity_tagged_fraud.stop()
        sinking_VelocityExpandeddf.stop()
        spark.stop()
    }
  }
}
