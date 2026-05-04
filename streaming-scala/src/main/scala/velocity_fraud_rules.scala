// importing packages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

// creating a singleton scala object containing the velocity fraud method

object velocity_fraud{
  def velocity_fraud_detection(df:DataFrame): DataFrame = {

    // extracting time from "transaction_time" column as new column called "event_time"
    // "event_time" column will have timestamp as datatype

    val event_time_df = df.withColumn("event_time", to_timestamp(col("transaction_time")))

    // since we got our "event_time" column in timestamp format
    // we can add watermark now, that will wait for 2 to 5 minutes before finalising the transaction window
    
    val watermarked_df = event_time_df.withWatermark("event_time","2 minutes")

    // now let's form a dataframe where transactions for each user_id is displayed
    // within a 60 seconds window and let's count the number of transactions
    // if the count of transactions for a particular user within a 60 seconds window > 5
    // that transaction should be an anomaly, and considered as velocity fraud

    val transactions_per_minute = watermarked_df.groupBy(col("user_id"), window(col("event_time"),"60 seconds"))
                                                .count()

    // filtering the count now
    
    val velocity_fraud_df = transactions_per_minute.filter(col("count") > 5 )

    velocity_fraud_df
  }
}
