import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStateByKeyWordCount {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[2]").setAppName("Wordcount")
        val ssc = new StreamingContext(conf, Seconds(1))

        // spark streaming 和 spark core 中的checkpoint的区别
        /**
          * spark core ： 将spark core 将数据持久化到外部文件系统中
          *
          * spark stream : 将上下文环境、未处理数据、以及数据持久化到外部文件中
          *
          * */
        ssc.checkpoint("hdfs://s100:8020/wordcount_checkpoint")


        val lines = ssc.socketTextStream("localhost", 9999)
        val words = lines.flatMap(_.split(" "))
        val pairs = words.map(word => (word, 1))
        val wordCount = pairs.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
            var newValue = state.getOrElse(0)
            for (value <- values) {
                newValue += value
            }
            Option(newValue)
        })
        wordCount.print()
        ssc.start()
        ssc.awaitTermination()
    }
}