package cn.cupcat

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
import scala.language.postfixOps


object SessionStat {


    def main(args: Array[String]): Unit = {

        //首先读取所有的限制
        val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        //由json转换为object
        val taskParm = JSONObject.fromObject(jsonStr)


        //    创建UUID
        val taskUUID = UUID.randomUUID().toString

        //创建sparkConf
        val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")

        //创建sparkSession
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        //获取动作表的数据
        val actionRDD = getBasicActionData(sparkSession, taskParm)

        //转换为kv形式的表
        val session2actionRDD = actionRDD.map(item => (item.session_id, item))
        //因为后期经常使用所以可以将数据缓存下
        session2actionRDD.cache()

        val session2ActionRDD = session2actionRDD.groupByKey()

        //提取数据并将数据转换为key 是userid
        val sessionId2FullInfoRDD = aggregateBySession(session2ActionRDD, sparkSession)

        val sessionStatisticAccumulater = new SessionStatAccumulator
        sparkSession.sparkContext.register(sessionStatisticAccumulater)


        //进行数据逻辑的操作
        val sessionId2FilterRDD = getfilterRDD(sparkSession, taskParm, sessionStatisticAccumulater, sessionId2FullInfoRDD)

        sessionId2FilterRDD.foreach(println(_))


        //统计各范围内的session的占比统计
        getSessionRatio(sparkSession, taskUUID, sessionStatisticAccumulater.value)

    }


    //进行占比的统计
    def getSessionRatio(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

        //首先获取session总的个数
        val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble


        //获取浏览的时长范围
        val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
        val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
        val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
        val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
        val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
        val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
        val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
        val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
        val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)


        //统计点击的步长（也就是个数）
        val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
        val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
        val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
        val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
        val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
        val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)


        //进行占比的计算
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

        //进行数据的封装

        val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
            visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
            visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
            step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
            step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)


        val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))

        import sparkSession.implicits._

        statRDD.toDF().write
            .format("jdbc")
            .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
            .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
            .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
            .option("dbtable", "session_stat")
            .mode(SaveMode.Append)
            .save()
    }

    //进行过滤并进行累加
    def getfilterRDD(sparkSession: SparkSession,
                     taskParam: JSONObject,
                     sessionStatisticAccumulater: SessionStatAccumulator,
                     sessionId2FullInfoRDD: RDD[(String, String)]) = {
        //首先提取参数限制条件
        val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
        val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
        val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
        val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
        val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
        val searchKeywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
        val clickCategories = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

        var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
            (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
            (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
            (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
            (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
            (if (searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
            (if (clickCategories != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategories else "")

        if (filterInfo.endsWith("\\|")) {
            filterInfo = filterInfo.substring(0, filterInfo.length - 1)
        }

        val sessionId2FullFilter = sessionId2FullInfoRDD.filter {
            case (sessionId, fullinfo) =>
                var success = true
                if (!ValidUtils.between(fullinfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                    success = false
                }
                if (!ValidUtils.in(fullinfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
                    success = false
                }
                if (!ValidUtils.in(fullinfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
                    success = false
                }

                if (!ValidUtils.equal(fullinfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
                    success = false
                }

                if (!ValidUtils.in(fullinfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
                    success = false

                if (!ValidUtils.in(fullinfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
                    success = false

                if (success) {
                    sessionStatisticAccumulater.add(Constants.SESSION_COUNT)

                    def calculateVisitLength(visitLength: Long): Unit = {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_1m_3m)
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_4s_6s)
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_7s_9s)
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_10s_30s)
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_30s_60s)
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_1m_3m)
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_3m_10m)
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_10m_30m)
                        } else if (visitLength > 1800) {
                            sessionStatisticAccumulater.add(Constants.TIME_PERIOD_30m)
                        }

                    }

                    def calculateStepLength(stepLength: Long): Unit = {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionStatisticAccumulater.add(Constants.STEP_PERIOD_1_3)
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionStatisticAccumulater.add(Constants.STEP_PERIOD_4_6)
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionStatisticAccumulater.add(Constants.STEP_PERIOD_7_9)
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionStatisticAccumulater.add(Constants.STEP_PERIOD_10_30)
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionStatisticAccumulater.add(Constants.STEP_PERIOD_30_60)
                        } else if (stepLength > 60) {
                            sessionStatisticAccumulater.add(Constants.STEP_PERIOD_60)
                        }
                    }

                    val stepLength = StringUtils.getFieldFromConcatString(fullinfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
                    val visitLength = StringUtils.getFieldFromConcatString(fullinfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
                    calculateVisitLength(visitLength)
                    calculateStepLength(stepLength)
                }
                success
        }
        sessionId2FullFilter
    }


    //提取动作行为里的数据
    def aggregateBySession(session2ActionRDD: RDD[(String, Iterable[UserVisitAction])], sparkSession: SparkSession) = {
        val userId2AggrInfoRDD = session2ActionRDD.map {
            case (sessionId, iterAction) =>
                var startime: Date = null
                var endtime: Date = null
                val searchKeywords = new StringBuffer("")
                val clickCategries = new StringBuffer("")
                var userId = -1L
                var stepLength = 0L

                for (action <- iterAction) {

                    //首先确定userid
                    if (userId == -1L) {
                        userId = action.user_id
                    }

                    //更新起始和结束时间
                    val actionTime = DateUtils.parseTime(action.action_time)

                    if (startime == null || startime.after(actionTime)) {
                        startime = actionTime
                    }

                    if (endtime == null || endtime.before(actionTime)) {
                        endtime = actionTime
                    }

                    //完成搜索关键字的追加 （去重）
                    val searkKeyWord = action.search_keyword
                    if (StringUtils.isNotEmpty(searkKeyWord) && !searchKeywords.toString.contains(searkKeyWord)) {
                        searchKeywords.append(searkKeyWord)
                    }

                    //完成点击品类的追加（去重）
                    val clickCategrie = action.click_category_id
                    if (clickCategrie != -1L && !clickCategries.toString.contains(clickCategrie)) {
                        clickCategries.append(clickCategrie)
                    }
                    stepLength += 1
                }

                val searchKW = StringUtils.trimComma(searchKeywords.toString)
                val clickCG = StringUtils.trimComma(clickCategries.toString)

                //获取访问的时长
                val visitLength = (endtime.getTime - startime.getTime) / 1000

                //返回拼接的数据
                val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
                    Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
                    Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" +
                    Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                    Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
                    Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startime)

                (userId, aggrInfo)
        }
        val sql = "select * from user_info"
        import sparkSession.implicits._

        val userId2UserInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(
            item => (item.user_id, item)
        )

        val sessionId2FullInfoRDD = userId2AggrInfoRDD.join(userId2UserInfoRDD).map {
            case (userId, (aggrInfo, userInfo)) =>
                val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
                    Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
                    Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
                    Constants.FIELD_CITY + "=" + userInfo.city

                val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

                (sessionId, fullInfo)
        }
        sessionId2FullInfoRDD
    }

    //获取用户的行为数据
    //UserVisitAction(2018-06-23,6,0c3d0fd33bf14042af20ed5ab6ca18f8,1,2018-06-23 6:27:16,新辣道鱼火锅,-1,-1,null,null,null,null,1)
    def getBasicActionData(sparkSession: SparkSession, taskParm: JSONObject) = {
        val startDate = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)

        val sql = "select * from user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "'"

        import sparkSession.implicits._

        sparkSession.sql(sql).as[UserVisitAction].rdd
    }

}
