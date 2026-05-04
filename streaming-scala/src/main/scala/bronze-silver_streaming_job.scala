// importing packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object, containing the main method

object bronze_silver_spark_streaming{
  def main(args: Array[String]){

    // instantiating a spark session
    val spark = SparkSession.builder().appName("bronze_to_silver").getOrCreate()

    // since the raw data is being persisted to bronze layer by kafka
    // we need to define the schema first to ingest that raw data before applying transformations

    val incoming_stream_schema = StructType(Array(StructField("value",StringType,false),
                                                  StructField("ingestion_time",TimestampType,false)
                                                  ))
    // now defining a streaming function, that will read from the bronze layer directory
    
    val incoming_stream = spark.readStream.format("parquet")
                                          .schema(incoming_stream_schema)
                                          .load("../data/raw_bronze_layer")

    // first sinking the incoming data stream to console, to verify the flow
   // val writing_incoming_stream=incoming_stream.writeStream.format("console")
   //                            .outputMode("append")
   //                            .option("truncate",false)
   //                            .option("checkpointLocation","/tmp/bronze-silver")
   //                            .start()

   // ************************ FLOW VERIFIED ********************************
   
    // defining the schema to be applied to incoming stream

    val dataframe_schema = StructType(Array(StructField("transaction_id",StringType,false),
                                            StructField("user_id",StringType,false),
                                            StructField("merchant_id",StringType,false),
                                            StructField("transaction_amount",DoubleType,false),
                                            StructField("transaction_time",StringType,false),
                                            StructField("device_id",StringType,false),
                                            StructField("location",StringType,false),
                                            StructField("is_international",BooleanType,false)
                                  ))

    // performing the KEY TRANSFORMATION to raw incoming stream
    // here, the whole raw data is transformed into a dataframe, showing clear attributes, holding values

    val transformed_incoming_stream = incoming_stream.select(from_json(col("value")
                                                      .cast("String"), dataframe_schema).alias("data")
                                                      ,col("ingestion_time"))

    // now the data is in "string" format in column named "data"
    // this returns the whole string as single column called "data" holding all values
    // we need to flatten it, therefore

    val transformed_final_df = transformed_incoming_stream.select("data.*", "ingestion_time")

    // ******************* KEY TRANSFORMATION PHASE COMPLETE ************************

    // persisting this transformed dataframe to silver layer storage following medallion architecture
    // storing in parquet format and since the data production & ingestion is in prototyping phase
    // therefore, not implementing patitioning in storage layers for now

    val persisting_transformed_df = transformed_final_df.writeStream.format("parquet")
                                           .outputMode("append")
                                           .option("path","../data/transformed_silver_layer")
                                           .option("checkpointLocation","/tmp/checkpoints-bronze_to_silver_parquet")
                                           .start()

    // sinking to console too for debugging purposes

    val sinking_to_console = transformed_final_df.writeStream
                                                 .format("console")
                                                 .outputMode("append")
                                                 .option("checkpointLocation","/tmp/checkpoints-bronze_to_silver")
                                                 .start()

    try {
      // writing_incoming_stream.awaitTermination()
      persisting_transformed_df.awaitTermination()
      sinking_to_console.awaitTermination()
    }
    catch{ case e: Exception =>
      println("Stopping stream and closing spark session")
      // writing_incoming_stream.stop()
      persisting_transformed_df.stop()
      sinking_to_console.stop()
      spark.stop()
    }
  }
}

