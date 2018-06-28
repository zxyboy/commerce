import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object PageConvertRate {

    def main(args: Array[String]): Unit = {
        val jsonStr  = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        //任务过滤条件
        val taskParam = JSONObject.fromObject(jsonStr)

        val taskUUID = UUID.randomUUID().toString

        val sparkConf = new SparkConf().setAppName("pageStatistics").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        //获取配置信息数据 String 1,2,3,4,5,6,7
        val pageFlowArray = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",")

        //配置页切片 : list(1_2,2_3,...)
        val pageSplitList = pageFlowArray.slice(0, pageFlowArray.length - 1)
            .zip(pageFlowArray.tail)
            .map(x => x._1 + "_" + x._2)

        val sessionId2ActionRDD = getSeesionId2ActionRDD(sparkSession, taskParam)

        //第一个页面访问的总数
        val firstPageCount = sessionId2ActionRDD.filter {
            case (pageSplit, action) =>
                action.page_id == pageFlowArray(0).toLong
        }.count()


        val pageSplit2CountMap = sessionId2ActionRDD.groupByKey()
            .flatMap {
                case (sessionId, iterableAction) =>
                    val sessionId2PageList = iterableAction.toList
                        .sortWith((action1, action2) => DateUtils.parseTime(action1.action_time).before(DateUtils.parseTime(action2.action_time)))
                        .map(_.page_id)

                    sessionId2PageList.take(sessionId2PageList.length - 1)
                        .zip(sessionId2PageList.tail)
                        .map(x => x._1 + "_" + x._2)
                        .filter(pageSplitList.contains)
                        .map((_, 1))
            }.countByKey()
        // 保存统计的比率
        val page2RateMap = new mutable.HashMap[String, Double]
        //遍历我们统计的页面顺序
        var lastPageCount = firstPageCount
        for (pageSplit <- pageSplitList) {
            val rate = pageSplit2CountMap(pageSplit) / lastPageCount.toDouble
            //保存和HashMap中
            page2RateMap(pageSplit) = rate
            lastPageCount = pageSplit2CountMap(pageSplit)
        }

        var rateStr = page2RateMap.map(x => x._1 + "=" + x._2)
            .mkString("|")

        val pageSplitConvertRate = PageSplitConvertRate(taskUUID, rateStr)
        import sparkSession.implicits._
        val dataFrame = sparkSession.sparkContext.makeRDD(Array(pageSplitConvertRate)).toDF()


        writeToMySQL(sparkSession, dataFrame, "tbl_rate0115")


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

    def getSeesionId2ActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {

        val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
        import sparkSession.implicits._
        val sql = "select * from user_visit_action where date >= '" + startDate + "' and date <= '" + endDate + "'"
        sparkSession.sql(sql).as[UserVisitAction].rdd.map(action => (action.session_id, action))
    }
}
