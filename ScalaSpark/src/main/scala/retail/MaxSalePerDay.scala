package retail

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MaxSalePerDay {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("MaxSalePerDay").master("local").getOrCreate()

    val custDF = spark.read.option("header", "true").option("delimiter", ",")
      .csv(getClass.getResource("/retail/customer.csv").getPath)

    val tranDF = spark.read.option("header","true").option("delimiter",",")
      .csv(getClass.getResource("/retail/transaction.csv").getPath)

    val midDF = custDF.join(tranDF,Seq("cust_id"),"inner")

//    midDF.show()
//
//    +-------+------+-------+----------+------+
//    |cust_id|  name|bill_id|      date|amount|
//    +-------+------+-------+----------+------+
//    |      1| Romio|     12|03-01-2020|   500|
//    |      1| Romio|     11|02-01-2020|   110|
//    |      1| Romio|     10|01-01-2020|   100|
//    |      2| Akbar|     20|02-01-2020|   200|
//    |      3|Walter|     32|03-01-2020|   400|
//    +-------+------+-------+----------+------+

    import org.apache.spark.sql.functions._
    val amountDF = midDF.groupBy("date").agg(max("amount").as("amount"))


//      +----------+----------+
//      |      date|max_amount|
//      +----------+----------+
//      |01-01-2020|       100|
//      |02-01-2020|       200|
//      |03-01-2020|       500|
//      +----------+----------+

   // val result =  amountDF.join(midDF,midDF("amount")===amountDF("max_amount")).select("cust_id","max_amount","date")
    val result = amountDF.join(midDF,Seq("amount"),"inner")
    result.show

    
//    +------+----------+-------+-----+-------+----------+
//    |amount|      date|cust_id| name|bill_id|      date|
//    +------+----------+-------+-----+-------+----------+
//    |   100|01-01-2020|      1|Romio|     10|01-01-2020|
//    |   200|02-01-2020|      2|Akbar|     20|02-01-2020|
//    |   500|03-01-2020|      1|Romio|     12|03-01-2020|
//    +------+----------+-------+-----+-------+----------+


  }

}
