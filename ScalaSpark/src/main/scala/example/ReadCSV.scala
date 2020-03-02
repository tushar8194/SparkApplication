package example

import org.apache.spark.sql.SparkSession

object ReadCSV {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("read_csv").master("local").getOrCreate()

    val df = spark.read.csv(getClass.getResource("/partition_data").getPath())

    df.show()

  }

}
