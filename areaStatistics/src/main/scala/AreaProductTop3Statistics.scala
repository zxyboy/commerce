import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object AreaProductTop3Statistics {


    def main(args: Array[String]): Unit = {
        val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
        val taskParam = JSONObject.fromObject(jsonStr)
        val taskUUID = UUID.randomUUID().toString
        val sparkConf = new SparkConf().setAppName("areaStatistics").setMaster("local[*]")

        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        val city2RowRDD = getCity2RowRDD(sparkSession, taskParam)

        val city2CityAreaRDD = getCity2CityAreaRDD(sparkSession)


        createCityAreaTempTable(sparkSession, city2RowRDD, city2CityAreaRDD)

        //注册一个UDF函数，作用为： 将输入的两个字符以指定的字符拼接
        sparkSession.udf.register("concat_long_string", (cityId: Long, cityName: String, split: String) => cityId + split + cityName)
        //注册一个聚合函数，作用为： 聚合所传入的字符串，字符串之间以逗号分隔
        sparkSession.udf.register("group_concat_str", new GroupConcatDistinct)
        //注册一个UDF函数，作用为： 获取保存在product_info表中的 extend_info 字段的值
        sparkSession.udf.register("get_json_value", (extract: String, field: String) => {
            val jsonObject = JSONObject.fromObject(extract)
            jsonObject.getString(field)
        })

        // 创建统计每个地区中每个产品id总和的临时表
        createTempTableAndAggrData(sparkSession)
        import sparkSession.implicits._

        val sql = "select area, " +
            "CASE " +
            "WHEN area='华北' OR area='华东' THEN 'A Level' " +
            "WHEN area='华南' OR area='华中' THEN 'B Level' " +
            "WHEN area='西南' OR area='西北' THEN 'C Level' " +
            "ELSE 'D Level' " +
            "END area_level, click_product_id, city_info, click_count, product_name, product_status from(" +
            " select area, click_product_id, city_info, click_count, product_name, product_status, " +
            " row_number() over(partition by area order by click_count desc) rank from tmp_area_product_info) t" +
            " where rank<=3"

//        val sql = "select area , " +
//            "CASE " + " " +
//            "WHEN area='华北' OR area ='华东' THEN 'A Level' " +
//            "WHEN area='华南' OR area='华中' THEN 'B Level' " +
//            "WHEN area='西南' OR area='西北' THEN 'C Level' " +
//            "ELSE 'D Level' " +
//            "END area_level, click_product_id, city_info, click_count, product_name, product_status from( " +
//            " select area, city_info, click_product_id, product_name, product_status,click_count, " +
//            " row_number() over(partition by area order by click_count desc) rank from tmp_area_product_info) t" +
//            "  where rank<=3"

        val dataFrame = sparkSession.sql(sql).rdd.map {
            row =>
                AreaTop3Product(taskUUID, row.getAs[String]("area"), row.getAs[String]("area_level"),
                    row.getAs[Long]("click_product_id"), row.getAs[String]("city_info"),
                    row.getAs[Long]("click_count"), row.getAs[String]("product_name"),
                    row.getAs[String]("product_status"))
        }.toDF()
        writeToMySQL(sparkSession, dataFrame, "area_top3_product0115")
        //sparkSession.sql("select * from tmp_area_product_info").show(20)


        sparkSession.stop()

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

    /**
      * 聚合数据 并且联立 product_info 表
      **/
    def createTempTableAndAggrData(sparkSession: SparkSession) = {
        var sql = "select area,click_product_id,count(*) click_count, " +
            " group_concat_str(concat_long_string(city_id,city_name,':')) city_info" +
            "  from temp_area_product_base_info " +
            " group by  area ,click_product_id"

        sparkSession.sql(sql).createOrReplaceTempView("temp_area_product_click_count")

        // 创建连接product_info 表，获取产品信息
        sql = "select tapcc.area,tapcc.click_product_id,tapcc.click_count,tapcc.city_info,pi.product_name,if(get_json_value(pi.extend_info,'product_status') ='0','Self','Third Party') product_status  from  temp_area_product_click_count tapcc join product_info pi on tapcc.click_product_id = pi.product_id"
        sparkSession.sql(sql).createOrReplaceTempView("tmp_area_product_info")
    }


    /**
      * 创建临时表 （"city_id", "city_name", "area", "click_product_id"）
      **/
    def createCityAreaTempTable(sparkSession: SparkSession, city2RowRDD: RDD[(Long, Row)], city2CityAreaRDD: RDD[(Long, Row)]) = {

        import sparkSession.implicits._

        val joinRDD = city2RowRDD.join(city2CityAreaRDD).map {
            case (cityId, (action, area)) =>
                val product_id = action.getAs[Long]("click_product_id")
                val city_name = area.getAs[String]("city_name")
                val areas = area.getAs[String]("area")
                (cityId, city_name, areas, product_id)
        }

        joinRDD.toDF("city_id", "city_name", "area", "click_product_id").createOrReplaceTempView("temp_area_product_base_info")
        //joinRDD.foreach(println)
        /* city2RowRDD.join(city2CityAreaRDD)
             .map {
                 case (cityId, (action, area)) =>
                     (cityId, area.getAs[String]("city_name"), area.getAs[String]("area"), action.getAs[Long]("click_product_id"))
             }.toDF("city_id", "city_name", "area", "click_product_id")*/

    }


    /**
      * 获取城市地区信息
      **/
    def getCity2CityAreaRDD(sparkSession: SparkSession) = {

        val cityAreaInfoArray = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"),
            (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"),
            (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"),
            (9L, "哈尔滨", "东北"))

        import sparkSession.implicits._
        sparkSession.sparkContext.makeRDD(cityAreaInfoArray)
            .toDF("city_id", "city_name", "area")
            .rdd
            .map(row => (row.getAs[Long]("city_id"), row))
    }


    /**
      * 获取用户访问行为信息
      **/
    def getCity2RowRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
        val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
        val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

        val sql = s"select click_product_id,city_id from user_visit_action where date >= '$startDate' and date <= '$endDate' " +
            s"and click_product_id != -1 and click_product_id is not null"

        sparkSession.sql(sql).rdd.map(row => (row.getAs[Long]("city_id"), row))

    }

}


