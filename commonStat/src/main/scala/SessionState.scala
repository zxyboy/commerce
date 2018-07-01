import java.util.UUID

import CommonObject.getSessionFullInfo
import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable

object SessionState {


    def main(args: Array[String]): Unit = {
        //需求：统计点击、下单、支付的 top10，并且按照点击、下单、支付的数量排序（降序）

        /**
          * 分析：
          *     1. 读取user_visit_action 表数据
          *     2. 获取所有点击、下单、支付数据的集合(categoryId,value) : 说明：value可以为任意值，需要对集合进行去重操作
          *     3. 获取点击动作的集合（categoryId,count）
          *     4. 获取下单动作集合(categoryId,count)
          *     5. 获取支付动作集合（categoryId,count）
          *     6. 联合数据 2 、3 、4 、5
          *     7. 创建二次排序类，定义排序规则（继承Ordered类）
          *     8. 修改联合数据的key为我们的排序类，进行排序
          **/
        val jsonConfigStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        val configObj = JSONObject.fromObject(jsonConfigStr)
        val uuid = UUID.randomUUID().toString
        val sparkConf = new SparkConf().setAppName("sessionCommon").setMaster("local[*]")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        val userVisitActionRDD = getUserVisitActionRDD(sparkSession, configObj)
        val sessionId2ActionRDD = userVisitActionRDD.map(item => (item.session_id, item))
        val sessionId2ActionGroupRDD = sessionId2ActionRDD.groupByKey()
        //持久化
        sessionId2ActionGroupRDD.persist()
        //查询user_info 表数据，并且和以上数据join
        val sessionId2FullInfoRDD = getSessionFullInfo(sparkSession, sessionId2ActionGroupRDD)

        val sessionId2FilterRDD = getFilterRDD(sessionId2FullInfoRDD, configObj)

        val sessionId2FilteredActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map {
            case (sessionId, (action, fullInof)) =>
                (sessionId, action)
        }


        val sc = sparkSession.sparkContext
        // 获取所有点击、下单、支付数据的集合 : (cid,cid)
        val allCategoryIdRDD = getAllCategoryRDD(userVisitActionRDD)
        //获取点击类型集合
        val clickCategoryIdMap = getClickCategoryMap(userVisitActionRDD)
        //获取下单类型集
        val orderCategoryIdMap = getOrderCategoryMap(userVisitActionRDD)
        //获取支付类型集合
        val payCategoryIdMap = getPayCategoryMap(userVisitActionRDD)
        // 混合数据
        val top10Map = allCategoryIdRDD.map {
            case (categoryId, cid) =>
                var aggrInfo = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|"
                aggrInfo += Constants.FIELD_CLICK_COUNT + "=" + clickCategoryIdMap.getOrElse(categoryId, 0) + "|"

                aggrInfo += Constants.FIELD_ORDER_COUNT + "=" + orderCategoryIdMap.getOrElse(categoryId, 0) + "|"

                aggrInfo += Constants.FIELD_PAY_COUNT + "=" + payCategoryIdMap.getOrElse(categoryId, 0) + "|"

                (categoryId, aggrInfo.substring(0, aggrInfo.length - 1))
        }.map {
            case (categoryId, fullInfo) =>
                val clickCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
                val orderCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
                val payCount = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
                (SortKey(clickCount, orderCount, payCount), fullInfo)
        }.sortByKey(false).take(10)
        top10Map.foreach(println)

        val top10RDD = sparkSession.sparkContext.makeRDD(top10Map).map {
            case (sortKey, fullInfo) =>
                val categoryId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
                val payCount = sortKey.payCount
                val orderCount = sortKey.orderCount
                val clickCount = sortKey.clickCount
                Top10Category(uuid, categoryId, clickCount, orderCount, payCount)
        }
        import sparkSession.implicits._
        val dataFrame = top10RDD.toDF()
        top10RDD.foreach(println(_))
        //writeToMySQL(sparkSession, dataFrame, "session_top10")

        // 需求四： 求top10热门类别Id的 top10点击的session


        //整理 top10的categoryId数据 (categoryId,categoryId)
        //        val top10CidToCidRDD = top10RDD.map(item => (item.categoryid, item.categoryid))
        //
        //        //整理过滤以后符合条件的数据
        //        // sessionId2FilterRDD => (sessionId,action)
        //        // 整理以后： （categoryId,action）
        //        val cid2ActionRDD = sessionId2FilteredActionRDD.map {
        //            case (sessionId, action) =>
        //                //这里是点击的类别id
        //                val cid = action.click_category_id
        //                (cid, action)
        //        }
        //        //方式一：
        //        // top10CidToCidRDD 和  sessionId2FilteredActionRDD 进行join操作
        //        // 得到数据结构： (sessionId + "_" + cid, 1)
        //        val cid2JoinActionRDD = top10CidToCidRDD.join(cid2ActionRDD).map {
        //            case (cid, (categoryId, action)) =>
        //                val sessionId = action.session_id
        //                (sessionId + "_" + cid, 1)
        //        }
        //        val dateFrame1 = cid2JoinActionRDD.reduceByKey(_ + _)
        //            .map {
        //                case (seesionIdAndCid, count) =>
        //                    val sessionId = seesionIdAndCid.split("_")(0)
        //                    val cid = seesionIdAndCid.split("_")(1)
        //                    (cid, (sessionId, count))
        //            }.groupByKey()
        //            .flatMap {
        //                case (cid, iterableTuple) =>
        //                    iterableTuple.toList.
        //                        sortWith((x, y) => x._2 > y._2)
        //                        .take(10).map {
        //                        case (sessionId, count) =>
        //                            Top10Session(uuid, cid.toLong, sessionId, count.toLong)
        //                    }
        //
        //            }.toDF()
        //
        //        writeToMySQL(sparkSession, dateFrame1, "tbl_sessiontop10_0115")


        // cid2JoinGroupActionRDD : （cid, Iterator[action,action,...]）
        /*         方式二 ：
                val cid2JoinActionRDD = top10CidToCidRDD.join(cid2ActionRDD).map {
                    case (cid, (categoryId, action)) =>
                        val sessionId = action.session_id
                        (sessionId, action)
                }

                val cid2JoinGroupActionRDD = cid2JoinActionRDD.groupByKey()
                val cid2SessionIdCountRDD = cid2JoinGroupActionRDD.flatMap {
                    case (sessionId, iterableAction) =>
                        val map = new mutable.HashMap[Long, Long]()

                        for (action <- iterableAction) {
                            val clickCategoryId = action.click_category_id
                            if (!map.contains(clickCategoryId)) {
                                map(clickCategoryId) = 0
                            }
                            map(clickCategoryId) = map(clickCategoryId) + 1
                        }

                        for ((cid, count) <- map) yield (cid, sessionId + "=" + count)
                }

                val frame = cid2SessionIdCountRDD.groupByKey().flatMap{
                    case (cid,iterableSesionIDCount) =>
                        iterableSesionIDCount.toList.sortWith((x,y) => x.split("=")(1).toLong > y.split("=")(1).toLong)
                        .take(10)
                        .map{
                            item => val sessionId = item.split("=")(0)
                                val count = item.split("=")(1).toLong
                                Top10Session(uuid,cid,sessionId,count)
                        }
                }.toDF()
                writeToMySQL(sparkSession,frame,"top10_session0115")*/
        sparkSession.close()
    }

    /**
      * 整理过滤条件
      **/
    // RDD(sessionId,fullInfo)
    def getFilterRDD(sessionId2FullInfoRDD: RDD[(String, String)], taskParam: JSONObject) = {

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
                success
        }

        sessionId2FilterRDD
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


    def getPayCategoryMap(userVisitActionRDD: RDD[UserVisitAction]) = {
        userVisitActionRDD.flatMap(x => {
            val pay_category_ids = x.pay_category_ids
            val map = new mutable.HashMap[Int, Int]
            if (pay_category_ids != null) {
                pay_category_ids.split(",").foreach(cid => {
                    map += (cid.toInt -> 1)
                })
            }
            map
        }).countByKey()
    }

    def getOrderCategoryMap(userVisitActionRDD: RDD[UserVisitAction]) = {
        userVisitActionRDD.flatMap(x => {
            val order_category_ids = x.order_category_ids
            val map = new mutable.HashMap[Int, Int]
            if (order_category_ids != null) {
                order_category_ids.split(",").foreach(cid => {
                    map += (cid.toInt -> 1)
                })
            }
            map
        }).countByKey() //.reduceByKey(_ + _) //
    }

    /**
      * 获取点击类型集合 (cid,count)
      **/
    def getClickCategoryMap(userVisitActionRDD: RDD[UserVisitAction]) = {
        userVisitActionRDD.map(userVisitAction => {
            // 订单的categoryId
            val clickCategoryId = userVisitAction.click_category_id.toInt
            if (clickCategoryId != -1) {
                (clickCategoryId, 1)
            } else {
                (clickCategoryId, 0)
            }
        }).filter(x => x._1 != -1 || x._1 != 0).countByKey() //.reduceByKey(_ + _) //.countByKey() 错误结果： (69,8)
    }

    //    def getOrderCategoryMap(userVisitActionRDD: RDD[UserVisitAction]) = {
    //        userVisitActionRDD.mapPartitions(iteratorUserVisitAction => {
    //            val map = new mutable.HashMap[Int, Int]()
    //            for (userVisitAction <- iteratorUserVisitAction) {
    //                // 支付的categoryId
    //                val order_category_ids = userVisitAction.order_category_ids
    //                if (order_category_ids != null) {
    //                    order_category_ids.split(",").foreach(cid => {
    //                        val iCid = cid.toInt
    //                        if (!map.contains(iCid)) {
    //                            map(iCid) = 0
    //                        }
    //                        map(iCid) = map(iCid) + 1
    //                    })
    //                }
    //            }
    //            map.toIterator
    //        }).reduceByKey(_ + _) //.countByKey()
    //    }
    //
    //    /**
    //      * 获取点击类型集合 (cid,count)
    //      **/
    //    def getClickCategoryMap(userVisitActionRDD: RDD[UserVisitAction]) = {
    //        userVisitActionRDD.mapPartitions(iteratorUserVisitAction => {
    //            val map = new mutable.ListMap[Int, Int]()
    //            for (userVisitAction <- iteratorUserVisitAction) {
    //                // 订单的categoryId
    //                val clickCategoryId = userVisitAction.click_category_id.toInt
    //                if (clickCategoryId != -1) {
    //                    if (!map.contains(clickCategoryId)) {
    //                        map(clickCategoryId) = 0
    //                    }
    //                    map(clickCategoryId) = map(clickCategoryId) + 1
    //                }
    //            }
    //            map.toIterator
    //        }).reduceByKey(_ + _) //.countByKey() 错误结果： (69,8)
    //    }


    /**
      * 获取所有点击、下单、支付数据的集合(categoryId,value) :
      **/
    def getAllCategoryRDD(userVisitActionRDD: RDD[UserVisitAction]) = {
        val allCategoryIdRDD = userVisitActionRDD.mapPartitions(iteratorUserVisitAction => {
            val map = new mutable.HashMap[Int, Int]()
            for (userVisitAction <- iteratorUserVisitAction) {

                //点击过的categoryId
                val clickCategoryId = userVisitAction.click_category_id.toInt
                if (clickCategoryId != -1) {
                    map += (clickCategoryId -> clickCategoryId)
                }
                // 订单的categoryId
                if (userVisitAction.order_category_ids != null) {
                    userVisitAction.order_category_ids.split(",").foreach(cid => map += (cid.toInt -> cid.toInt))
                }
                // 支付的categoryId
                if (userVisitAction.pay_category_ids != null) {
                    userVisitAction.pay_category_ids.split(",").foreach(cid => map += (cid.toInt -> cid.toInt))
                }
            }
            map.toIterator
        }).distinct()
        allCategoryIdRDD
    }


    /**
      * 获取带有过滤日期的RDD数据
      **/
    def getUserVisitActionRDD(sparkSession: SparkSession, configObj: JSONObject) = {
        val startDate = configObj.getString(Constants.PARAM_START_DATE)
        val endDate = configObj.getString(Constants.PARAM_END_DATE)

        val sql = "select * from user_visit_action where action_time >= '" + startDate + "' and action_time <= '" + endDate + "'"
        import sparkSession.implicits._
        sparkSession.sql(sql).as[UserVisitAction].rdd
    }
}
