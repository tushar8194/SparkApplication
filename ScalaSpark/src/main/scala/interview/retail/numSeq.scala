package interview.retail

object numSeq {

  def main(args: Array[String]): Unit = {

    //Task1

    //Create a sequence from 1 to 10. (seq1)
    val num = Seq(1,2,3,4,5,6,7,8,9,10)

    //Convert the seq1 to List. (list1)
    val newList1 = num.toList
    println(newList1)

    //Add 11 to list1 at the end. (list2)
    val newList2 = newList1 :+ 11
    println(newList2)

    //Add 0 to the beginning of the list2. (list3)
    val newList3 = newList2.::(0)
    //val newList3 = 0 :: newList2
    println(newList3)

    //Take 5th element from list3 and append to end of list3. (list4)
    //// val newList4 = newList3 :+ newList3.slice(4,5).head
    val newList4 = newList3 :+ newList3.lift(4).get
    println(newList4)

    //Concatenate list2 and list4 (resultOfTask1)
    //// val res = newList2 ++ newList4
    val res = newList2 :+ newList4
    println(res)

    //Create a array from 12 to 99.(arr1)
    val arr : Array[Int] = 12 to 99 toArray

    //Convert arr1 to List (list5)
    val arrList = arr.toList
    println(arrList)


    //Task 2

    //    Create a List from 1 to 100.(list1)



    //    Multiply all the elements with 2.(list2)



    //    Get all the numbers that are divisible by 5 from list2.(list3)



    //    From list3, find the maximum, minimum, average, number of elements in the list, sum of all elements in the list ; Return the result as Tuple in the format "(bigger, smaller, average, number of elements, sum)".(resultOfTask2)


  }

}
