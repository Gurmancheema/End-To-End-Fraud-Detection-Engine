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

   // reading the high_amount_fraud files

   val high_amount_df = spark.read.format("parquet")
                                  .option("header","true")
                                  .option("inferschema","true")
                                  .load("../data/gold_layer/high_amount_fraud")

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


    // standardizing the high_amount dataframe

    val highamountClean = high_amount_df.withColumn("triggered_rule",lit("HIGH_AMOUNT"))
                                      .select("transaction_id",
                                              "user_id",
                                              "merchant_id",
                                              "transaction_amount",
                                              "transaction_time",
                                              "device_id",
                                              "location",
                                              "is_international",
                                              "ingestion_time",
                                              "triggered_rule")

    // Now performing the UNION operation between two dataframes to form a single fraud containing dataframe

    val union_df = highamountClean.unionByName(velocityClean)

    // Since now the columns are union (stacked vertically by Name), we need to aggregate the rows
    // and use collect_set on "triggered_rule" column

    val final_union_df = union_df.groupBy(col("transaction_id"),
                                          col("user_id"),
                                          col("merchant_id"),
                                          col("transaction_amount"),
                                          col("transaction_time"),
                                          col("device_id"),
                                          col("location"),
                                          col("is_international"),
                                          col("ingestion_time"))
                                  .agg(collect_set("triggered_rule").alias("triggered_rules"))

   // persisting this dataframe to gold storage layer
  
   final_union_df.write.format("parquet").mode("append").save("../data/gold_layer/fraud_union/")


    // print schemas
    // velocityExpandeddf.printSchema()
    velocityClean.printSchema()
    //high_amount_df.printSchema()
    highamountClean.printSchema()
    final_union_df.printSchema()

    // display dataframes 
    velocityExpandeddf.show()
    velocityClean.show()
    final_union_df.show()

    //stop the spark session
    spark.stop()
  }
}
