package interview.retail

import scala.util.Random
import scala.util.control.Breaks._


object UniqeNumbersWithSumZero {

//  Problem : Number must be random unique and sum equals to zero.


  def main(args: Array[String]): Unit = {

    solution(3).foreach(println)
  }



  def solution(n: Int) : Array[Int] ={

    val rand = new Random()
    val retrn = new Array[Int](n)
    var sum = 0

    for(i <- 0 until(n-1)){
      var num  = rand.nextInt(n + 100)

      breakable {
        while (true) {
          if (!retrn.contains(num)) {
            retrn(i) = num
            sum = sum + num
            break
          } else {
            num = rand.nextInt(n + 100)
          }
        }
      }

    }
    retrn(n-1) = sum * -1
    retrn
  }




}
