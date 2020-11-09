package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ReplaceValueInCol {

  // Replace column values on condition

  // Tip : if we keep old and new column name same than only new column will appear in results.

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("ReplaceColumnValue").getOrCreate()

    //val df = spark.createDataFrame([(1,"a"),(2,"a"),(3,"222"),(4,"2222")],["id","d_id"])

    import spark.implicits._
    val df = Seq((1,"a"),(2,"a"),(3,"222"),(4,"2222")).toDF("id","d_id")


    df.show()

    val newDF = df.withColumn("d_id", regexp_replace(col("d_id"), "a", "0"))

    newDF.show()
  }



}
