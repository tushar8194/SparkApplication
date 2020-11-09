package example

import org.apache.spark.sql.SparkSession

object RDDAggrigate {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("RddAggrigate").master("local").getOrCreate()

    val inputrdd = spark.sparkContext.parallelize(List(("maths", 21), ("english", 22), ("science", 31)), 3)

    val result = inputrdd.aggregate(0) ( (acc, value) => (acc + value._2),(acc1, acc2) => (acc1 + acc2) )


    println("result : " + result)
  }

}
