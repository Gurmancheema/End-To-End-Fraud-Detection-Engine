//import packages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method

object read_parquet{
  def main(args:Array[String]){

    // instantiating the spark session
    val spark = SparkSession.builder().appName("reading_parquet").getOrCreate()

    // reading the velocity_ fraud files

    val velocity_df = spark.read.format("parquet")
                        .option("header","true")
                        .option("inferschema","true")
                        .load("../data/gold_layer/velocity_fraud")

   // reading the saved files from silver layer

   val silver_stream =  spark.read.format("parquet")
                                  .option("header","true")
                                  .option("inferschema","true")
                                  .load("../data/transformed_silver_layer")

   // merging the cleaned & transformed dataframe from silver layer with velocity_df
   // to fetch the exact transactions and related data to the fraud

    val velocityExpandeddf = velocity_df
                                .withColumn("start", col("window.start"))
                                .withColumn("end", col("window.end"))
                                .drop("window")
                                .alias("v")
                                .join(
                                  silver_stream.alias("s"),
                                  col("v.user_id") === col("s.user_id") &&
                                  to_timestamp(col("s.transaction_time"))
                                    .between(col("v.start"), col("v.end"))
                                )

   // standardizing this velocity dataframe before merging with highamount dataframe

    val velocityClean = velocityExpandeddf.withColumn("triggered_rule", lit("HIGH_VELOCITY"))
                                      .select(
                                        col("s.transaction_id"),
                                        col("v.user_id"),
                                        col("s.merchant_id"),
                                        col("s.transaction_amount"),
                                        col("s.transaction_time"),
                                        col("s.device_id"),
                                        col("s.location"),
                                        col("s.is_international"),
                                        col("s.ingestion_time"),
                                        col("triggered_rule")
                                      )
   

    velocity_df.show()
    velocityClean.show()
    velocityExpandeddf.show()


    // display dataframe and print inferred schema
    velocity_df.printSchema()
    silver_stream.printSchema()
    velocityExpandeddf.printSchema()
    velocity_df.printSchema()

    //stop the spark session
    spark.stop()
  }
}
