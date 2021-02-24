package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.IntegerType



object TransformColumn {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val saprk : SparkSession = SparkSession.builder().appName("Reading File using Dataframe").master("local").getOrCreate()

    val filePath : String = getClass.getResource("/FileForDF.txt").getPath

    val df : DataFrame = saprk.read.options(Map("inferSchema" -> "false", "sep" -> "|","header" -> "true")).csv(filePath)


    // function 1
    df.show(false)

    df
      .withColumnRenamed("CustId","CustIdNew")
      .withColumnRenamed("Items","ItemsNew")
      .show(false)

    computeColumnRename(df ,  Map(("CustId","CustIdNew" ), ("Items","ItemsNew")) ).show(false)



    // function 2
    df.printSchema()

    df
      .withColumn("testCol",lit("Null"))
      .withColumn("CustId",col("CustId").cast(IntegerType))
      .printSchema()

    computeColumn(df, Map(("testCol",lit("Null")),("CustId",col("CustId").cast(IntegerType)))).printSchema()


  }



  def computeColumnRename(df: DataFrame, colExprs: Map[String, String]) : DataFrame = {
    colExprs.foldLeft(df) { (df, expr) =>
      df.withColumnRenamed(expr._1,expr._2)
    }
  }

  def computeColumn(df: DataFrame, colExprs: Map[String, Column]) : DataFrame = {
    colExprs.foldLeft(df) { (df, expr) =>
      df.withColumn(expr._1,expr._2)
    }
  }

}



/*

RESULTS :

Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
+------+-----------+-----+
|CustId|Items      |Price|
+------+-----------+-----+
|123   |baby powder|50   |
|123   |shampoo    |175  |
|475   |Cookies    |89   |
|689   |Scissors   |40   |
+------+-----------+-----+

+---------+-----------+-----+
|CustIdNew|ItemsNew   |Price|
+---------+-----------+-----+
|123      |baby powder|50   |
|123      |shampoo    |175  |
|475      |Cookies    |89   |
|689      |Scissors   |40   |
+---------+-----------+-----+

+---------+-----------+-----+
|CustIdNew|ItemsNew   |Price|
+---------+-----------+-----+
|123      |baby powder|50   |
|123      |shampoo    |175  |
|475      |Cookies    |89   |
|689      |Scissors   |40   |
+---------+-----------+-----+

root
 |-- CustId: string (nullable = true)
 |-- Items: string (nullable = true)
 |-- Price: string (nullable = true)

root
 |-- CustId: integer (nullable = true)
 |-- Items: string (nullable = true)
 |-- Price: string (nullable = true)
 |-- testCol: string (nullable = false)

root
 |-- CustId: integer (nullable = true)
 |-- Items: string (nullable = true)
 |-- Price: string (nullable = true)
 |-- testCol: string (nullable = false)


Process finished with exit code 0

*/
