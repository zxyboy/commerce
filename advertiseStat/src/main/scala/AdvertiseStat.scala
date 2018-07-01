import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object AdvertiseStat {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))


        val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
        val kafka_topics = ConfigurationManager.config.getString("kafka.topics")

        val kafkaParam = Map(
            "bootstrap.servers" -> kafka_brokers,
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "group1",
            // largest   smallest   none
            // latest    earilst    none
            // latest:   在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
            //           如果没有offset，就从最新的数据开始消费
            // earlist: 在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
            //           如果没有offset，就从最早的数据开始消费
            // none:    在消费指定topic的分区时，如果已经有offset，直接使用之前保存的offset，
            //           如果没有offset，直接报错
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        // 获取spark-stream  kafka消费者
        val adRealTimeDataDStream = KafkaUtils.createDirectStream(streamingContext,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))


        // 获取kafka消费者value数据
        val adRealTimeValueDStream = adRealTimeDataDStream.map(_.value())


        //过滤掉黑名单数据,得到没有添加到黑名单的数据
        val userId2RecordFilterRDD = filterBlackList(sparkSession, adRealTimeValueDStream)


        //需求7. 统计每个人每天点击每一类广告的数量和，如果每天某个人点击某一类广告超过100，则将其加入黑名单
        //加入黑名单的用户，点击的广告不会参与到统计结果中
        //将过滤到的数据更新写入到MySQL数据库中 查询写入以后最新的数据，判断有没有超过预定阈值的数据，如果有，则添加到黑名单
        //writeToMySQLAndCheckBlackList(sparkSession, userId2RecordFilterRDD)
        // 需求8. 实时统计每天各省各城市点击广告的数量
        //streamingContext.checkpoint("./checkpoint.log")
        //val perProvinceCityAdCountDStream = statPerProvinceCityAdCount(userId2RecordFilterRDD)

        //需求9 : 实时统计每天每个省点击广告Top3
        //statPerDayProvinceAdCount(sparkSession, perProvinceCityAdCountDStream)

        //需求10 实时统计每隔1小时广告点击
        perHourAdClickCount(userId2RecordFilterRDD)


        streamingContext.start()
        streamingContext.awaitTermination()

    }

    /**
      * 统计每小时每分钟内各个各个广告点击的次数
      **/
    def perHourAdClickCount(userId2RecordFilterRDD: DStream[(Long, String)]) = {
        val hourMiniteDSteam = userId2RecordFilterRDD.map {
            case (userId, record) =>
                //timestamp 	  province 	  city        userid         adid
                val recordArray = record.split(" ")
                val timestamp = recordArray(0).toLong
                val hourMinite = DateUtils.formatTimeMinute(new Date(timestamp))
                val adid = recordArray(4)
                (hourMinite + "_" + adid, 1L)
        }

        // 使用窗口函数统计， 设置滑动步长为1分钟， 窗口大小为60分钟
        val key2ClickCountDStream = hourMiniteDSteam.reduceByKeyAndWindow((a: Long, b: Long) => a + b,
            Minutes(60),
            Minutes(1))

        key2ClickCountDStream.foreachRDD {
            rdd =>
                rdd.foreachPartition {
                    val clickTrendArray = new mutable.ArrayBuffer[AdClickTrend]()
                    items =>
                        //  (hourMinite + "_" + adid, 1L)  格式化时间，保留到分钟级别 yyyyMMddHHmm
                        for (item <- items) {
                            val keySplit = item._1.split("_")
                            // yyyyMMddHHmm
                            val timeMinute = keySplit(0)
                            val date = timeMinute.substring(0, 8)
                            val hour = timeMinute.substring(8, 10)
                            val minute = timeMinute.substring(10)
                            val adid = keySplit(1).toLong

                            val clickCount = item._2

                            clickTrendArray += AdClickTrend(date, hour, minute, adid, clickCount)

                        }
                        AdClickTrendDAO.updateBatch(clickTrendArray.toArray)
                }
        }
    }

    def statPerDayProvinceAdCount(sparkSession: SparkSession, perProvinceCityAdCountDStream: DStream[(String, Long)]) = {
        // (combineKey,count)  combineKey  date + "_" + province + "_" + city + "_" + adid
        val top3Adevertist = perProvinceCityAdCountDStream.transform {
            rdd =>
                val key2tableRDD = rdd.map {
                    case (combineKey, count) =>
                        val combineKeyArray = combineKey.split("_")
                        val date = combineKeyArray(0)
                        val province = combineKeyArray(1)
                        val adid = combineKeyArray(3)
                        val key = date + "_" + province + "_" + adid
                        (key, count)
                }.reduceByKey(_ + _)
                    .map {
                        case (key, count) =>
                            val keySplitArray = key.split("_")
                            val date = keySplitArray(0)
                            val province = keySplitArray(1)
                            val adid = keySplitArray(2).toLong
                            (date, province, adid, count)
                    }
                import sparkSession.implicits._
                key2tableRDD.toDF("date", "province", "adid", "count")
                    .createOrReplaceTempView("tmp_province_click_count")

                val sql = "select date,province,adid,count from ( " +
                    " select date,province,adid,count, row_number() over(partition by province order by count desc ) rank  " +
                    " from tmp_province_click_count) t " +
                    " where rank <= 3 "
                sparkSession.sql(sql).rdd
        }

        //写入到MySQL中
        top3Adevertist.foreachRDD {
            rdd =>
                rdd.foreachPartition {
                    items =>
                        val top3Array = new mutable.ArrayBuffer[AdProvinceTop3]()
                        for (item <- items) {
                            val date = item.getAs[String]("date")
                            val province = item.getAs[String]("province")
                            val adid = item.getAs[Long]("adid")
                            val clickCount = item.getAs[Long]("count")
                            top3Array += AdProvinceTop3(date, province, adid, clickCount)

                        }
                        AdProvinceTop3DAO.updateBatch(top3Array.toArray)

                }
        }
    }

    /**
      * 实时统计每天每个省每个城市点击广告的总数
      *
      * 分析：
      * 粒度： day  provice city product
      *
      *   1. 将day  provice city product 拼成组合key， 并且使用带有状态的算子，统计出数量
      **/
    def statPerProvinceCityAdCount(userId2RecordFilterRDD: DStream[(Long, String)]) = {

        //userId2RecordFilterRDD : (userId,record)  =》 record timestamp province city userid adid
        //  映射成组合key
        val combineKey2NumDStream = userId2RecordFilterRDD.map {
            case (userId, record) =>
                val splitArray = record.split(" ")
                val timeStamp = splitArray(0).toLong
                val date = DateUtils.formatDate(new Date(timeStamp))
                val province = splitArray(1)
                val city = splitArray(2)
                val adid = splitArray(4)
                // 组合key
                val combineKey = date + "_" + province + "_" + city + "_" + adid
                (combineKey, 1L)
        }

        //实时统计
        val statResultDStream = combineKey2NumDStream.updateStateByKey[Long] {
            (valueSeq: Seq[Long], stat: Option[Long]) =>
                var statValue = stat.getOrElse(0L)
                statValue += valueSeq.sum
                Some(statValue)
        }
        //写入到数据库
        statResultDStream.foreachRDD {
            rdd =>
                rdd.foreachPartition {
                    //(combineKey,count) :  combineKey ：   date + "_" + province + "_" + city + "_" + adid
                    iteratorKV =>
                        val arrayBuffer = new ArrayBuffer[AdStat]()
                        iteratorKV.foreach {
                            case (combineKey, count) =>
                                val splitKeyArray = combineKey.split("_")
                                val date = splitKeyArray(0)
                                val province = splitKeyArray(1)
                                val city = splitKeyArray(2)
                                val adid = splitKeyArray(3)
                                arrayBuffer += AdStat(date, province, city, adid.toLong, count)
                        }
                        AdStatDAO.updateBatch(arrayBuffer.toArray)
                }
        }
        statResultDStream
    }


    /**
      * //将过滤到的数据汇总以后更新写入到MySQL数据库中
      * 查询写入以后最新的数据，判断有没有超过预定阈值的数据，如果有，则添加到黑名单
      **/
    def writeToMySQLAndCheckBlackList(sparkSession: SparkSession, userId2RecordFilterRDD: DStream[(Long, String)]) = {

        //需求：每天每个人点击每一个广告的次数超过100 则添加到黑名单， 添加到黑名单的用户不能参与点击广告统计
        //timestamp 	  province 	  city        userid         adid
        //(0,1530346105342 2 2 0 3)
        val key2CountDStream = userId2RecordFilterRDD.map {
            case (userId, record) =>
                val recordSplit = record.split(" ")
                // 日期格式 yyyy-MM-dd
                val date = DateUtils.formatDate(new Date(recordSplit(0).toLong))
                //广告id
                val adid = recordSplit(4).toLong
                val combineKey = date + "_" + userId + "_" + adid
                (combineKey, 1L)

        }.reduceByKey(_ + _)




        // 统计，写入到mysql
        key2CountDStream.foreachRDD {
            rdd =>
                //保存数据
                rdd.foreachPartition {
                    iteratorKV =>
                        val adUserClickCounts = new ArrayBuffer[AdUserClickCount]()
                        for (kv <- iteratorKV) {
                            val count = kv._2
                            val keySplitArray = kv._1.split("_")
                            val date = keySplitArray(0)
                            val userId = keySplitArray(1).toLong
                            val adid = keySplitArray(2).toLong

                            adUserClickCounts += AdUserClickCount(date, userId, adid, count)
                        }
                        println("---------------------开始写入数据-------------------------------")
                        adUserClickCounts.foreach(println)
                        //写入MySQL中
                        AdUserClickCountDAO.updateBatch(adUserClickCounts.toArray)
                }
        }
        //检查是否需要添加到黑名单
        key2CountDStream.filter {
            case (key, count) =>
                val keySplit = key.split("_")
                val dateKey = keySplit(0)
                val userId = keySplit(1).toLong
                val adid = keySplit(2).toLong
                val count = AdUserClickCountDAO.findClickCountByMultiKey(dateKey, userId, adid)

                if (count >= 100) {
                    true
                } else {
                    false
                }

        }.map {
            case (key, count) =>
                val userId = key.split("_")(1).toLong
                userId
        }.transform((rdd => rdd.distinct()))
            .foreachRDD {
                rdd =>

                    val blackListArray = new ArrayBuffer[AdBlacklist]()
                    rdd.foreachPartition {
                        items =>
                            for (item <- items) {
                                blackListArray += AdBlacklist(item)
                            }
                    }
                    AdBlacklistDAO.insertBatch(blackListArray.toArray)
            }


    }

    def filterBlackList(sparkSession: SparkSession, adRealTimeValueDStream: DStream[String]) = {

        //过滤掉黑名单数据 1530345630266 9 9 4 2
        //timestamp 	  province 	  city        userid         adid
        adRealTimeValueDStream.transform(recordRdd => {

            val blackLists = AdBlacklistDAO.findAll()
            val blackListUserRDD = sparkSession.sparkContext.makeRDD(blackLists).map(adBlackList => (adBlackList.userid, true))

            val userId2RecordRDD = recordRdd.map(item => (item.split(" ")(3).toLong, item))

            userId2RecordRDD.leftOuterJoin(blackListUserRDD).filter {
                case (userId, (record, bool)) =>
                    if (bool.isDefined && bool.get) {
                        false
                    } else {
                        true
                    }
            }.map {
                case (userId, (record, bool)) => (userId, record)
            }
        })


    }

    /**
      * 写入数据到数据库
      **/
    def writeToMySQL(sparkSession: SparkSession, dataFrame: DataFrame, tableName: String) = {
        dataFrame.write.format("jdbc")
            .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
            .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
            .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
            .option("dbtable", tableName)
            .mode(SaveMode.Append)
            .save()
    }
}
