package cn.cupcat

import scala.collection.mutable

object TestMap {
    def main(args: Array[String]): Unit = {
        val map = new mutable.HashMap[Int, Int]()

        map += (1 -> 2)
        map += (1 -> 3)
        map += (2 -> 2)
        map += (2 -> 3)

        for ((k, v) <- map) {
            println(s"k = $k  v = $v")
        }
    }
}
