// import packges
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.PrintWriter


// creating a singleton scala object containing the main method
object bigquery_transactions{
  def main(args:Array[String]){

    // creating a spark session
    val spark = SparkSession.builder().appName("all_transactions_to_bq").getOrCreate()

    // ingesting the data from silver layer
    val df = spark.read.format("parquet").option("inferschema","true")
                                .load("../data/transformed_silver_layer")

    // display dataframe & verify schema
    df.printSchema()
    df.show()

    // since the "transaction_time" is being inferred as "STRING" by spark
    // changing that to "TIMESTAMP" datatype

    val cleaned_df = df.withColumn("transaction_time", to_timestamp(col("transaction_time"),
                                                          "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
    cleaned_df.printSchema()

    // In order to push the new records only to Bigquery, i need a mechanism that checks the last batch push
    // from local silver layer storage to Bigquery
    // Therefore, concept of checkpointing comes in place
    // For every batch push to Bigquery, i will create checkpoints consisting of timestamps (ingestion_time)
    // So only records with latest timestamps than the checkpoint will be pushed to BigQuery

    // **************** IMPLEMENTING CHECKPOINTING USING TIMESTAMPS ********************************

    // fetch the latest "ingestion_time" from loaded batch data to store it as a checkpoint

    val df_max_timestamp = cleaned_df.select(max(col("ingestion_time")).alias("max_ingestion_time")).collect()(0)
                                    .getTimestamp(0)

            // .select(max()) still returns a DataFrame, not a Scala value
            // Spark is lazy & distributed — actual value sits across worker nodes
            // array indexing — grabs first (and only) Row from the Array
            // pulls all rows from distributed DataFrame to driver as Array[Row]
            // max() guarantees 1 row, but Spark still returns an Array)
            // Row is generic, Spark doesn't know the type inside
            // .getTimestamp(0) extracts column at index 0 as java.sql.Timestamp
            // argument 0 is column index, not row index)
    
    println(s"The latest timestamp from loaded batch data is: ${df_max_timestamp}")

    // let's check the checkpoint location, for an already saved checkpoint
    // If there is already a checkpoint timestamp present, then I will arithmetically compare them
    // So, if the checkpoint timestamp = loaded batch data's max timestamp, that means no new data has arrived
    // CASE 2: If there is no checkpoint timestamp present, that means batch data is being pushed first time to BQ
    // CASE 3: If the checkpoint timestamp < loaded batch data's maxx timestamp, that means HAS arrived

    try
    {
    val checkpoint_timestamp = spark.read.json("/home/gurman/End-To-End-Fraud-Detection-Engine/orchestration/bq_storage_checkpoint/")
    checkpoint_timestamp.printSchema()

    // this returns a dataframe, but the column value is in "STRING" format
    // so in order to compare two timestamps, I need to cast this STRING to timestamp format first

    val filtered_checkpoint_timestamp = checkpoint_timestamp.select(col("last_ingestion_time").cast("timestamp"))
                                                  .collect()(0).getTimestamp(0)

    println(s"The latest checkpointed timestamp is: ${filtered_checkpoint_timestamp}")

    // now comparing the timestamps, fetched from checkpoint & the data batch one

    if (filtered_checkpoint_timestamp.compareTo(df_max_timestamp) >=0){
      println("No New data arrived yet in silver layer storage :(")
    }
    else {
      println("New data has arrrived :) Connecting to BigQuery")
      pushtoBigQuery(cleaned_df,filtered_checkpoint_timestamp,spark)
    }
    }
    catch {
      case e: Exception =>
        println("--------No checkpoint timestamp found ----------")
        pushtoBigQuery(cleaned_df, null, spark, isFirstRun = true)

    }
  }

  def pushtoBigQuery(df:DataFrame, checkpointTime:java.sql.Timestamp, spark:SparkSession, isFirstRun:Boolean =false):
  Unit = {


    // since the dataframe's schema is clean, checkpoints are up to the point
    // let's define GCP credentials & connect to BigQuery

    val bq_project = "fraud-detection-engine-0001"
    val bq_dataset = "fraud_detection"
    val bq_table = "all_transactions"
    val gcp_key_path = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", 
                        throw new RuntimeException("GCP credentials are not set"))

    // if first run — push everything, no filtering needed
    // if not first run — filter only new records since last checkpoint

    val records_to_push = if (isFirstRun) {
                              println("First run, pushing all records to BigQuery")
                              df
                            }
                          else {
                            println("Incremental Run -- pushing new records only")

                            // filter only the new records from the last saved checkpoint
                            df.filter(col("ingestion_time") > lit(checkpointTime))
                          }

    println(s"No. of records to push: ${records_to_push.count()}")

    // pushing this filtered data to bigquery

    records_to_push.write.format("bigquery").mode("append")
                                  .option("table",s"${bq_project}.${bq_dataset}.${bq_table}")
                                  .option("writeMethod","direct")
                                  .save()

    println("Data pushed to Bigquery successfully! :)")

    // Since the data is pushed to Bigquery, now it's time to update the checkpoint timestamp in local VM
    // therefore, extract the max "ingestion_time" from the pushed records

    val latest_timestamp_to_checkpoint = records_to_push.select(max(col("ingestion_time")))
                                                        .collect()(0).getTimestamp(0)
    println(s"New checkpoint to save : ${latest_timestamp_to_checkpoint}")

    // create dataframe from "latest_timestamp_to_checkpoint" scalar value

    import spark.implicits._
    val created_checkpoint_df = Seq(latest_timestamp_to_checkpoint).toDF("last_ingestion_time")

    // write checkpoint in JSON format
    created_checkpoint_df.write.mode("overwrite")
                         .json("/home/gurman/End-To-End-Fraud-Detection-Engine/orchestration/bq_storage_checkpoint/")
    println("Checkpoint saved!")



    //stop the spark session
    spark.stop()
  }
}
