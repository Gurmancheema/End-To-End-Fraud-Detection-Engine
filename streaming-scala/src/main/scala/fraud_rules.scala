// this scala script defines the set of rules
// if any rule is violated, that transaction is flagged as "fraudulent"

// importing packages

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

// creating a singleton scala object containing the function that flags fraud transactions

object fraud_rules{
  def apply_rules(df:DataFrame): DataFrame = {

    // Stateless rule if "transaction amount" is more than set threshold & transaction is "international"
    // then the new column "is_fraud" shall label that transaction as true else false

    val applied_rules_df = df.withColumn("high_amount_flag",col("transaction_amount") > 20000)
                             .withColumn("intl_flag",col("is_international") === true)
                             .withColumn("is_fraud", col("high_amount_flag") && col("intl_flag"))

    // now since all the transactions have been labeled with "is_fraud" column, let's filter
    // out transactions for which it is labelled "true"

    val filtered_df = applied_rules_df.filter(col("is_fraud") === true)

    filtered_df

  }
}
