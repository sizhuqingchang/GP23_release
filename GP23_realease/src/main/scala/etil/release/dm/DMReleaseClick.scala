package etil.release.dm

import com.qf.bigdata.release.util.CommonUtil
import constant.ReleaseConstant
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.SparkHelper

import scala.collection.mutable.ArrayBuffer

class DMReleaseClick {

}
object DMReleaseClick {

  val logger: Logger = Logger.getLogger(DMReleaseClick.getClass)
  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String): Unit = {

    val begin = System.currentTimeMillis()

    spark.udf.register("getAgeRange", CommonUtil.getAgeRange(_: String))

    try {

      //设置缓存级别
      val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode = SaveMode.Overwrite

      //导入隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //获取字段
      val clickDWColumns = DMReleaseColumnsHelper.selectDWClickColumns()
      val customerColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()

      //设置条件
      val clickCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)
     // val customerCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)

      //获取join条件
      val joinColumns: Seq[String] = Seq[String](
        s"${ReleaseConstant.COL_RELEASE_SESSION_DEVICE_NUM}",
        s"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
        s"${ReleaseConstant.COL_RELEASE_SOURCES}",
        s"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        s"${ReleaseConstant.DEF_PARTITION}"
      )

      //读取hive表数据
      val clickDF= SparkHelper.readTable(spark,ReleaseConstant.DW_RELEASE_CLICK,clickDWColumns)
        .join(SparkHelper.readTable(spark,ReleaseConstant.DW_RELEASE_CUSTOMER,customerColumns)
        ,joinColumns)
        .where(clickCondition)
        .persist(storageLevel)

      //获取聚合字段
      val clickGroupColumns= Seq[Column](
        $"${ReleaseConstant.COL_RELEASE_SOURCES}",
        $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
        $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
        $"${ReleaseConstant.COL_RELEASE_AID}",
        $"${ReleaseConstant.DEF_PARTITION}"
      )

      //DM层字段

      val clickDMColumns=DMReleaseColumnsHelper.selectDMClickColumns()

      val clickDMDF= clickDF.groupBy(clickGroupColumns:_*)
        .agg(
          count(col(ReleaseConstant.COL_RELEASE_AID))
            .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
        )
        .selectExpr(clickDMColumns:_*)

      // clickDMDF.show(20,false)
      SparkHelper.writeTableData(clickDMDF,ReleaseConstant.DM_RELEASE_CLICK_CUBE,saveMode)

    }
     catch{
       case ex:Exception=>{
            logger.error(ex.getMessage,ex)
       }
     }
  finally{
    // 任务处理的时长
    println(s"任务处理时长：${appName},bdp_day = ${bdp_day}, ${System.currentTimeMillis() - begin}")
  }

 }

  def handleJob(appName:String,bdp_day_begin:String,bdp_day_end:String)= {
     var spark: SparkSession = null
     try {
     //配置spark
        val conf = new SparkConf()
            .set("hive.exec.dynamic.partition", "true")
            .set("hive.exec.dynamic.partition.mode", "nonstrict")
            .set("spark.sql.shuffle.partitions", "32")
            .set("hive.merge.mapfiles", "true")
            .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
            .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
            .set("spark.sql.crossJoin.enabled", "true")
            .setAppName(appName)
            .setMaster("local[*]")

        // 创建上下文
        spark = SparkHelper.createSpark(conf)

        // 解析参数
        val timeRange = SparkHelper.rangeDates(bdp_day_begin, bdp_day_end)

        // 循环参数
        for (bdp_day <- timeRange) {
           val bdp_date = bdp_day.toString
           handleReleaseJob(spark, appName, bdp_date)
        }

     }catch {
        case ex: Exception => {
            logger.error(ex.getMessage,ex)
        }
     }
  }

   def main(args: Array[String]): Unit = {
     val appName = "dw_release_job"
     val bdp_day_begin = "2019-09-24"
     val bdp_day_end = "2019-09-25"
     // 执行Job
     handleJob(appName,bdp_day_begin,bdp_day_end)
   }
}