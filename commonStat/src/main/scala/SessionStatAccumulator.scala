import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 自定义累加器
  **/
class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

    val countMap = new mutable.HashMap[String, Int]()


    override def isZero: Boolean = countMap.size == 0

    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
        val sessionStatAccumulator = new SessionStatAccumulator
        sessionStatAccumulator.countMap ++= this.countMap
        sessionStatAccumulator
    }

    override def reset(): Unit = this.countMap.clear

    override def add(v: String): Unit = {
        if (!this.countMap.contains(v)) {
            this.countMap += (v -> 0)
        }
        this.countMap.update(v, this.countMap.getOrElse(v, 0) + 1)
    }

    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
        other match {
            case acc: SessionStatAccumulator =>
                acc.countMap.foldLeft(this.countMap) {
                    case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
                }
            case _ => throw new RuntimeException(other.getClass.getName + "类型不匹配 !")
        }
    }

    override def value: mutable.HashMap[String, Int] = this.countMap
}
