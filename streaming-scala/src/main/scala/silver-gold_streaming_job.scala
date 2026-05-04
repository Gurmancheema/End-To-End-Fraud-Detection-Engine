// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method

object silver_gold_streaming{
  def main(args:Array[String]) {

    // instantiating a spark session
    val spark = SparkSession.builder().appName("silver_to_gold_streaming").getOrCreate()

    // defining a schema to read the transformed dataframe from the bronze layer

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
    

    val incoming_stream_from_bronze = spark.readStream.format("parquet")
                                                      .schema(schema)
                                                      .load("../data/transformed_silver_layer")

    // sinking this read dataframe to console to verify before applying transformations

    val sinking_stream_to_console = incoming_stream_from_bronze.writeStream
                                                               .format("console")
                                                               .outputMode("append")
                                                               .option("truncate",false)
                                                               .option("checkpointLocation","/tmp/checkpoints-sink")
                                                               .start()
    try {
      sinking_stream_to_console.awaitTermination()
    }
    catch {
      case e: Exception =>
        println("stopping write stream and spark session")
        spark.stop()
    }
  }
}
