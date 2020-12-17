package example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ImplicitMagic {


  implicit class richDF(df: DataFrame) {

    def getCount = df.count()

    def diffSchemaUnion(other: DataFrame) = {
      val dfcol = df.columns.toSet
      val otherCol = other.columns.toSet
      val allCol = dfcol ++ otherCol


      def expr(myCols: Set[String], allCols: Set[String]) = allCols.toList.map {
        case v if myCols.contains(v) => col(v)
        case v => lit(null).as(v)
      }

      df.select(expr(dfcol, allCol): _*).union(other.select(expr(otherCol, allCol): _*))
    }


  }

}


class ImplicitMagic {

  import ImplicitMagic._

  def main(args: Array[String]): Unit = {

    val df1  : DataFrame= ???

    val df2 : DataFrame= ???

    df1.getCount

    df1.diffSchemaUnion(df2)

  }
}
