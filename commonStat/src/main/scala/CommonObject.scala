import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object CommonObject {


    def main(args: Array[String]): Unit = {
        //加载配置信息
        val configJsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        val taskParam = JSONObject.fromObject(configJsonStr)

        // 创建UUID
        val taskUUID = UUID.randomUUID.toString
        //创建SparkConf对象

        val sparkConf = new SparkConf().setAppName("commonStat").setMaster("local[*]")
        //创建SparkSession对象
        val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport().getOrCreate()
        // 查看user_visit_action数据（查询指定日期）、
        //UserVisitAction(2018-06-25,96,870e807858bb4658a858adaa68bddb9b,3,2018-06-25 16:16:36,null,-1,-1,51,32,null,null,4)
        val actionRDD = getBasicActionData(sparkSession, taskParam)
        // 按照sessionId 聚合, 并返回（userId, Items）
        val sessionId2ActionRDD = actionRDD.map(item => (item.session_id, item))
        //(990012735627446ea0faadbeb7343217,CompactBuffer(UserVisitAction(2018-06-25,90,990012735627446ea0faadbeb7343217,1,2018-06-25 17:50:06,null,8,30,null,null,null,null,8), UserVisitAction(2018-06-25,90,990012735627446ea0faadbeb7343217,4,2018-06-25 17:39:08,null,-1,-1,37,60,null,null,0), UserVisitAction(2018-06-25,90,990012735627446ea0faadbeb7343217,8,2018-06-25 17:46:35,null,-1,-1,15,26,null,null,3)))
        val sessionId2ActionGroupRDD = sessionId2ActionRDD.groupByKey()
        //持久化
        sessionId2ActionGroupRDD.persist()

        //查询user_info 表数据，并且和以上数据join
        val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2ActionGroupRDD)

        val accumulator = new SessionStatAccumulator

        sparkSession.sparkContext.register(accumulator)
        //过滤数据
        val sessionId2FilterRDD = getFilterRDD(sessionId2FullInfoRDD, taskParam, accumulator)
        //获取统计结果
        sessionId2FilterRDD.foreach(println)

        //getSessionRatio(sparkSession, taskUUID, accumulator.value)
        // 需求二 ：随机抽取session
        // 将sessionId为key的数据，转换为dateHour为key的数据 (dateHour,fullInfo)
        val dateHour2FullInfoRDD = sessionId2FilterRDD.map {
            case (sessionId, fullInfo) =>
                val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
                // yyyy-MM-dd_HH
                val dateHour = DateUtils.getDateHour(startTime)
                (dateHour, fullInfo)
        }
        // mutable.HashMap[String,Long]   (dateHour,count)
        val dataHourToCount = dateHour2FullInfoRDD.countByKey()

        val dateToHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()
        //拆分，得到以下数据结构 (date,(hour,count))
        dataHourToCount.foreach {
            case (dateHour, count) => {
                val dateHourArray = dateHour.split("_")
                val date = dateHourArray(0)
                val hour = dateHourArray(1)
                dateToHourCountMap.get(date) match {
                    case None => dateToHourCountMap(date) = new mutable.HashMap[String, Long]()
                        dateToHourCountMap(date) += (hour -> count)
                    case Some(map) => dateToHourCountMap(date) += (hour -> count)
                }
            }
        }

        val dateToHourListMap = new mutable.HashMap[String, mutable.HashMap[String, List[Int]]]()
        // 拆分得到以下结构：(data,(hour,List(index1,index2,...)))
        //获取每一天抽取session的数量
        val sessionCountEachDay = 100 / dateToHourCountMap.size

        val random = new Random
        dateToHourCountMap.foreach {
            case (date, hourCountMap) =>
                //获取每天的总session个数
                val dateSessionSum = hourCountMap.values.sum
                dateToHourListMap.get(date) match {
                    case None => dateToHourListMap(date) = new mutable.HashMap[String, List[Int]]()
                        for ((hour, count) <- hourCountMap) {
                            // 获取每一天中每一小时抽取session的数量
                            var sessionCountEachHour = (count / dateSessionSum.toDouble * sessionCountEachDay).toInt
                            if (sessionCountEachHour > dateSessionSum) {
                                sessionCountEachHour = dateSessionSum.toInt
                            }
                            val list = new ListBuffer[Int]
                            // 生成索引
                            for (index <- 0 until sessionCountEachHour) {
                                list += random.nextInt(sessionCountEachHour)
                            }
                            dateToHourListMap(date) += (hour -> list.toList)
                        }
                    case Some(map) =>
                        for ((hour, count) <- hourCountMap) {
                            // 获取每一天中每一小时抽取session的数量
                            val sessionCountEachHour = (count / dateSessionSum.toDouble * sessionCountEachDay).toInt
                            val list = new ListBuffer[Int]
                            // 生成随机索引
                            for (index <- 0 until sessionCountEachHour) {
                                var randomIndex = random.nextInt(sessionCountEachHour)
                                // 避免重复索引
                                while (list.contains(randomIndex)) {
                                    randomIndex = random.nextInt(sessionCountEachHour)
                                }
                                list += randomIndex
                            }
                            dateToHourListMap(date) += (hour -> list.toList)
                        }
                }
        }


        val dateHour2GroupFullInfoRDD = dateHour2FullInfoRDD.groupByKey()

        val extractSessionRDD = dateHour2GroupFullInfoRDD.flatMap {
            case (dateHour, iterableFullInfo) =>
                val date = dateHour.split("_")(0)
                val hour = dateHour.split("_")(1)

                //获取小时生成的随机索引列表
                val indexList = dateToHourListMap.get(date).get(hour)

                val sessionRandomExtracts = new mutable.ArrayBuffer[SessionRandomExtract]()

                var index = 0
                // 判断当前索引是否包含在随机索引列表中
                for (fullInfo <- iterableFullInfo) {
                    // 存在，则提取元素
                    if (indexList.contains(index)) {
                        val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
                        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
                        val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
                        val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
                        //保存到集合中
                        sessionRandomExtracts += SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
                    }
                    index += 1
                }

                sessionRandomExtracts
        }

        import sparkSession.implicits._
        extractSessionRDD.toDF().write
            .format("jdbc")
            .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
            .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
            .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
            .option("dbtable", "session_random_extract_0115")
            .mode(SaveMode.Append)
            .save()
    }


    /**
      * 将统计到的结果写入mysql数据库
      **/
    def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {
        //注意： 这里一定要转换为double类型，不然得到的比率都为0
        val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble


        val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
        val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
        val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
        val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
        val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
        val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
        val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
        val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
        val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

        val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
        val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
        val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
        val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
        val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
        val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)


        val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
        val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
        val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
        val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
        val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
        val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
        val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
        val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
        val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

        val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
        val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
        val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
        val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
        val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
        val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

        val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
            visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
            visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
            step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
            step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)
        import sparkSession.implicits._

        val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))

        statRDD.toDF().write
            .format("jdbc")
            .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
            .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
            .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
            .option("dbtable", "session_stat_0115")
            .mode(SaveMode.Append)
            .save()
    }

    // RDD(sessionId,fullInfo)
    def getFilterRDD(sessionId2FullInfoRDD: RDD[(String, String)], taskParam: JSONObject, sessionStatisticAccumulator: SessionStatAccumulator) = {

        val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
        val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
        val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
        val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
        val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
        val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
        val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

        val filterField = Constants.PARAM_START_AGE + "=" + startAge + "|" +
            Constants.PARAM_END_AGE + "=" + endAge + "|" +
            Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" +
            Constants.PARAM_CITIES + "=" + cities + "|" +
            Constants.PARAM_SEX + "=" + sex + "|" +
            Constants.PARAM_KEYWORDS + "=" + keywords + "|" +
            Constants.PARAM_CATEGORY_IDS + "=" + categoryIds
        val sessionId2FilterRDD = sessionId2FullInfoRDD.filter {
            case (sessionId, fullInfo) =>
                //默认为true，表示通过过滤
                var success = true
                if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterField, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    success = false
                }
                if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterField, Constants.PARAM_PROFESSIONALS)) {
                    success = false
                }
                if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterField, Constants.PARAM_CITIES)) {
                    success = false
                }

                if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterField, Constants.PARAM_SEX)) {
                    success = false
                }

                if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterField, Constants.PARAM_KEYWORDS)) {
                    success = false
                }
                if (!ValidUtils.in(fullInfo, Constants.FIELD_CLICK_CATEGORY_IDS, filterField, Constants.PARAM_CATEGORY_IDS)) {
                    success = false
                }


                if (success) {
                    // 累加总条数
                    sessionStatisticAccumulator.add(Constants.SESSION_COUNT)

                    // 判断访问时长 累加visit_length
                    def calculateVisitLength(visitLength: Long): Unit = {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
                        } else if (visitLength > 1800) {
                            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
                        }
                    }

                    // 判断步长， 累加step_length
                    def calculateStepLength(stepLength: Long): Unit = {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
                        } else if (stepLength > 60) {
                            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
                        }
                    }

                    val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
                    val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
                    calculateStepLength(stepLength)
                    calculateVisitLength(visitLength)

                }

                success
        }

        sessionId2FilterRDD
    }

    def getSessionFullInfo(sparkSession: SparkSession, sessionId2ActionGroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
        val userId2AggrInfoRDD = sessionId2ActionGroupRDD.map {
            case (sessionId, iterableAction) =>
                var startTime: Date = null
                var endTime: Date = null
                val searchKeyworks = new StringBuffer("")
                val clickCategries = new StringBuffer("")
                var userId = -1L
                var stepLength = 0L

                for (action <- iterableAction) {
                    //第一次设置用户id
                    if (userId == -1L) {
                        userId = action.user_id
                    }
                    val actionTime = DateUtils.parseTime(action.action_time)
                    //设置开始时间
                    if (startTime == null || startTime.after(actionTime)) {
                        startTime = actionTime
                    }
                    //设置结束时间
                    if (endTime == null || endTime.before(actionTime)) {
                        endTime = actionTime
                    }

                    //设置搜索关键词
                    val search_keyword = action.search_keyword
                    if (StringUtils.isNotEmpty(search_keyword) && !searchKeyworks.toString.contains(search_keyword)) {
                        searchKeyworks.append(search_keyword + ",")
                    }
                    //设置点击品类
                    val click_category_id = action.click_category_id
                    if (click_category_id != -1 && !clickCategries.toString.contains(click_category_id)) {
                        clickCategries.append(click_category_id + ",")
                    }
                    stepLength += 1
                }
                //去除最后的逗号
                val searchKWs = StringUtils.trimComma(searchKeyworks.toString)
                val clickCGs = StringUtils.trimComma(clickCategries.toString)
                // 获取访问时长单位（s）
                val visitLength = (endTime.getTime - startTime.getTime) / 1000
                // 字段名=字段值|字段名=字段值|
                val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                    Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKWs + "|" +
                    Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCGs + "|" +
                    Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                    Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                    Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

                (userId, aggrInfo)
        }

        val sql = "select * from user_info"
        import sparkSession.implicits._
        val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

        val sessionId2FullInfoRDD = userId2UserInfoRDD.join(userId2AggrInfoRDD).map {
            case (userId, (userInfo, aggrInfo)) =>
                val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
                    Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
                    Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
                    Constants.FIELD_CITY + "=" + userInfo.city

                // 字段名=字段值|字段名=字段值|。。
                val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
                (sessionId, fullInfo)
        }
        sessionId2FullInfoRDD
    }


    def getBasicActionData(sparkSession: SparkSession, taskParam: JSONObject) = {
        val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

        val sql = "select * from  user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "'"
        import sparkSession.implicits._
        sparkSession.sql(sql).as[UserVisitAction].rdd
    }

}
