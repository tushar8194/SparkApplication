package example

import org.apache.spark.sql.SparkSession

object FlattenRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Faltten-RDD").master("local").getOrCreate()

    val x = spark.sparkContext.parallelize(List(("A", Set(1,2))))
    val y = spark.sparkContext.parallelize(List(("A", Set(3,4))))

    val arr = Array(x,y)

    val res = arr.reduce(_ ++ _).map(x => (x._1, x._2.head, x._2.last))

    res.collect().foreach(println)

//    (A,1,2)
//    (A,3,4)



  }

}
