package retail

object coinSequense {


  /* Problem : Arrays having coins in position, heads(0) and tail(1)
  *             two consecutive coins must not be same, return the number of flips
  *             required to correct the sequense */


  def main(args: Array[String]): Unit = {

    print(solution(Array(1,0,1,0,1,1)))
  }


  def solution(n: Array[Int]) : Int ={

    val curr= n(0)

    val next = if(curr == 0 ) 1 else 0

    var count = 0

    for(i <- 0 until(n.length-1) by 2) {

      if(n(i) != curr) {
        count = count +1
      }
      if(n(i+1) != next) {
        count = count +1
      }


    }

    count

  }

}
