package twitter

object findElementInfintieArrScala {

  def main(args: Array[String]): Unit = {

    val inf = Int.MaxValue

    val A = Array(1,4,6,8, inf,inf,inf,inf,inf,inf,inf)

    val res = findElement(A,80)

    println(res)

  }



  def findElement(array_of_elemets: Array[Int], x: Int) : Int = {
    /*
        var i =0

       while (array_of_elemets(i) != x) {
          if(array_of_elemets(i) == Int.MaxValue){
            return -1
          }
          else {
            i = i+1
          }
        }*/

    val rtrn = java.util.Arrays.binarySearch(array_of_elemets,x)

    if(rtrn < 0 ) {
      return -1
    }else{
      return rtrn
    }

  }

}
