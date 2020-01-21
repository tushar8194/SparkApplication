package exampleBasics

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col


object PrintEvenColumns {
  def main(args: Array[String]): Unit = {
    /*val spark=  SparkSession.builder().master("local").getOrCreate()

    val path=getClass.getResource("/EmployeeDetails.json").getPath

    val empDf=spark.read.json(path)

    val parentColumns= empDf.columns.toList

    import spark.implicits._



    //println(parentColumns.zipWithIndex)
    //List((deptno,0), (designation,1), (empno,2), (ename,3), (hire_date,4), (manager,5), (sal,6))

   // println(parentColumns.zipWithIndex.filter(_._2 % 2 == 0))
    //List((deptno,0), (empno,2), (hire_date,4), (sal,6))

   // println(parentColumns.zipWithIndex.map{case (row, index) =>  (row, index +1)}.filter(_._2 % 2 == 0).map(_._1))


val evencols = parentColumns.zipWithIndex.map{case(row,index) => (row,index+1)}.filter(_._2 % 2==0).map(_._1).toSeq

    //empDf.select("\""+evencols.tail.mkString("\",\"") + "\"").show()
    empDf.repartition(4).
*/

  }

}
