package exampleBasics

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

object ReadWriteAvroFile {

  def main(args: Array[String]): Unit = {

   // val logger = Logger.getLogger(getClass.getName)


    val spark = SparkSession.builder().master("local").getOrCreate()

    val csvDF = spark.read.option("header",true).csv("/Users/tarzan/code/IdeaProjects/SparkAppliction/ScalaSpark/src/main/resources/country.csv")

    println("" + csvDF.show())

    //csvDF.write.format("avro").save("/Users/tarzan/code/IdeaProjects/SparkAppliction/ScalaSpark/src/main/resources/country.avro")

    val avroDF = spark.read.format("avro").load("/Users/tarzan/code/IdeaProjects/SparkAppliction/ScalaSpark/src/main/resources/country.avro")

    println(""+avroDF.show())


  }

}
