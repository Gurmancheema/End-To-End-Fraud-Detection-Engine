// import packges
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

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
    df.select("transaction_time").show(false)

    // since the "transaction_time" is being inferred as "STRING"
    // changing that to "TIMESTAMP" datatype

    val cleaned_df = df.withColumn("transaction_time", to_timestamp(col("transaction_time"),
                                                  "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
    cleaned_df.show()
    cleaned_df.printSchema()

    // since the dataframe's schema is clean now,
    // let's define GCP credentials
    // to push the data to bigquery

    val bq_project = "fraud-detection-engine-0001"
    val bq_dataset = "fraud_detection"
    val bq_table = "all_transactions"
    val gcp_key_path = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS", 
                        throw new RuntimeException("GCP credentials are not set"))

    // pushing this data to bigquery

    cleaned_df.write.format("bigquery").mode("append")
                                  .option("table",s"${bq_project}.${bq_dataset}.${bq_table}")
                                  .option("writeMethod","direct")
                                  .save()

    // total rows pushed to bigquery
    println("Data pushed to Bigquery successfully")
    //stop the spark session
    spark.stop()
  }
}
