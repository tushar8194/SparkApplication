package example

object testTuring {

  def main(args: Array[String]): Unit = {

    val word = ""

    val newWord = word.toUpperCase()

    (for ( x <- 0 to word.length/2) yield (word(x) == word(word.length -x -1)) ).reduceLeft((acc,n) => acc && n)


  }

  val str  = Array("","")

  str(0).charAt(0)

}
