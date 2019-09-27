package etil.release.dm

import com.qf.bigdata.release.util.CommonUtil
import constant.ReleaseConstant
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import utils.SparkHelper

import scala.collection.mutable.ArrayBuffer

class DMReleaseRegister {

}
object DMReleaseRegister{
  val logger: Logger = Logger.getLogger(DMReleaseRegister.getClass)

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
      val registerDWColumns = DMReleaseColumnsHelper.selectDWRegisterColumns()

      val registeredColumns= DMReleaseColumnsHelper.selectOdsRegisteredColumns()
      //设置条件
      val registerCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)

      val registerDF= SparkHelper.readTable(spark,ReleaseConstant.DW_RELEASE_REGISTER,registerDWColumns)

      val registerjoinDF= registerDF.join(SparkHelper.readTable(spark, ReleaseConstant.ODS_RELEASE_USER, registeredColumns)
        , ReleaseConstant.COL_RELEASE_USER_ID)
        .where(registerCondition)
        .persist(storageLevel)

      //获取分组字段

     // registerjoinDF.groupBy()



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

    }
    catch {
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
