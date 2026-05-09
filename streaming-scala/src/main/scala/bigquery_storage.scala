// import packages
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// creating a singleton scala object containing the main method

object bigquery_storage{
  def main(args:Array[String]) {

    // defining some Bigquery variables used later in following code

    val bq_project = "fraud-detection-engine-0001"
    val bq_dataset = "fraud_detection"
    val bq_table_name = "fraud_transactions"
    val gcp_key_path = sys.env.getOrElse("GOOGLE_APPLICATION_CREDENTIALS"
                                        , throw new RuntimeException("GCP credentials are not set"))
    // instantiating a spark session
    val spark = SparkSession.builder()
                            .appName("bigquery_push")
                            .config("spark.sql.shuffle.partitions","4") // reducing partitions from default 200 to 4
                            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ingesting the stored gold layer data

    val gold_data = spark.read.format("parquet")
                              .option("inferschema","true")
                              .load("../data/gold_layer/fraud_union/")

    println(s"Rows read from gold layer storage: ${gold_data.count()}")

    // pushing this data to bigquery

    gold_data.write.format("bigquery").mode("append")
                                       .option("table",s"${bq_project}.${bq_dataset}.${bq_table_name}")
                                       .option("writeMethod","direct")
                                       .save()
    println("Data pushed to Bigquery")
    //stop the spark session
    spark.stop()
  }
}

