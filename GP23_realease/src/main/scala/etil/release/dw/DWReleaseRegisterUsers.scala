package etil.release.dw

import com.qf.bigdata.release.enums.ReleaseStatusEnum
import constant.ReleaseConstant
import etil.release.dw.DWRealeaseExposure.{handleJob, logger}
import etil.release.dw.DWReleaseCustomer.{handleReleaseJob, logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}
import utils.SparkHelper

import scala.collection.mutable.ArrayBuffer

class DWReleaseRegisterUsers {

}
object  DWReleaseRegisterUsers{
  private val logger: Logger = LoggerFactory.getLogger(DWReleaseRegisterUsers.getClass)

  def handleReleaseJob(spark: SparkSession, appName: String, bdp_day: String) = {

    //获取当前时间
    val begin: Long = System.currentTimeMillis()

    try {

      //设置隐式转换
      import spark.implicits._
      import org.apache.spark.sql.functions._

      //设置缓存级别
      val store: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
      val saveMode: SaveMode = SaveMode.Overwrite

      //获取字段
      val registerColumns: ArrayBuffer[String] = DWReleaseColumnsHelper.selectDWReleaseRegisterColumns()

      //设置条件
      val customerReleaseCondition = (col(s"${ReleaseConstant.DEF_PARTITION}") === lit(bdp_day)
        and
        col(s"${ReleaseConstant.COL_RELEASE_SESSION_STATUS}")
          === lit(ReleaseStatusEnum.REGISTER.getCode))

      val registerReleaseDF: Dataset[Row] = SparkHelper.readTable(spark, ReleaseConstant.ODS_RELEASE_SESSION,registerColumns)
        .where(customerReleaseCondition)
        .repartition(ReleaseConstant.DEF_REPARTITION_NUMBER)

      //展示
     // registerReleaseDF.show(20,false)

      //存储
      SparkHelper.writeTableData(registerReleaseDF,ReleaseConstant.DW_RELEASE_REGISTER,saveMode)

    }
    catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)

      }
    }
    finally {
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

    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage, ex)
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