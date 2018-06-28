object QuickSort {
    def quickSort(list: List[Int]): List[Int] = {
        list match {
            case Nil => Nil
            case List() => List()
            case head :: tail =>
                val a = tail.partition(_ < head)
                quickSort(a._1) ::: head :: quickSort(a._2)
        }
    }

    def main(args: Array[String]) {
        val list = List(3, 12, 43, 23, 7, 1, 2, 0)
        list.partition(_ < 23)
        println(quickSort(list))
    }
}
