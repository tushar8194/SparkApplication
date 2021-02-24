package logging.log4j

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object testLog {

  val logger : Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test slf4j").master("local").getOrCreate()

    import spark.implicits._

    val df= Seq((1,"a"),(2,"b"),(3,"c")).toDF("id","name")

    logger.info("My own df count is " + df.count)
    logger.info("My Own DF data is " + df.show())

  }

}
