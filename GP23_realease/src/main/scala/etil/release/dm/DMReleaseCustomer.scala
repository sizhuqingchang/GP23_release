package etil.release.dm

import com.qf.bigdata.release.util.CommonUtil
import constant.ReleaseConstant
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import utils.SparkHelper

import scala.collection.mutable.ArrayBuffer

class DMReleaseCustomer {

}

object  DMReleaseCustomer{

  //处理日志
  val logger: Logger = Logger.getLogger(DMReleaseCustomer.getClass)

  def handleReleaseJob(spark:SparkSession,appName:String,bdp_day:String): Unit ={

  val begin=System.currentTimeMillis()

  spark.udf.register("getAgeRange",CommonUtil.getAgeRange(_:String))

  try{

    //设置缓存级别
    val storageLevel: StorageLevel = ReleaseConstant.DEF_STORAGE_LEVEL
    val saveMode = SaveMode.Overwrite

    //导入隐式转换
    import spark.implicits._
    import  org.apache.spark.sql.functions._

    //获取字段
    val customerColumns: ArrayBuffer[String] = DMReleaseColumnsHelper.selectDWReleaseCustomerColumns()

    //设置条件
    val customerCondition =col(s"${ReleaseConstant.DEF_PARTITION}")===lit(bdp_day)

    //获取hive表数据
    val customerDF: Dataset[Row] = SparkHelper.readTable(spark, ReleaseConstant.DW_RELEASE_CUSTOMER, customerColumns)
     //加载条件
      .where(customerCondition)
      //缓存
      .persist(storageLevel)

    //展示
    customerDF.show(20,false)

   //获取维度列
    val customerSourceGroupColnmus= Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}"
    )



    //获取聚合列
    val customerSourceColumns = DMReleaseColumnsHelper.selectDMCustomerSourceColumns()

    val customerSourceDMDF = customerDF.groupBy(customerSourceGroupColnmus: _*)
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_SESSION_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(col(ReleaseConstant.COL_RELEASE_SESSION_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
      )
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}", lit(bdp_day))
      .selectExpr(customerSourceColumns: _*)

    customerSourceDMDF.show(10)

    //写入hive表
    SparkHelper.writeTableData(customerSourceDMDF,ReleaseConstant.DM_RELEASE_CUSTOMER_SOURCES,saveMode)


    // 目标客户多维度分析统计
    val customerGroupColumns = Seq[Column](
      $"${ReleaseConstant.COL_RELEASE_SOURCES}",
      $"${ReleaseConstant.COL_RELEASE_CHANNELS}",
      $"${ReleaseConstant.COL_RELEASE_DEVICE_TYPE}",
      $"${ReleaseConstant.COL_RELEASE_AGE_RANGE}",
      $"${ReleaseConstant.COL_RELEASE_GENDER}",
      $"${ReleaseConstant.COL_RELEASE_AREA_CODE}"
    )

    // 插入列
    val customerCubeColumns = DMReleaseColumnsHelper.selectDMCustomerCudeColumns()

    val customerCubeDMDF= customerDF.groupBy(customerGroupColumns:_*)
      .agg(
        countDistinct(col(ReleaseConstant.COL_RELEASE_SESSION_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_USER_COUNT}"),
        count(col(ReleaseConstant.COL_RELEASE_SESSION_DEVICE_NUM))
          .alias(s"${ReleaseConstant.COL_RELEASE_TOTAL_COUNT}")
      )
      .withColumn(s"${ReleaseConstant.DEF_PARTITION}",lit(bdp_day))
      .selectExpr(customerCubeColumns:_*)


    customerCubeDMDF.show(20,false)

    //写入hive表
    SparkHelper.writeTableData(customerCubeDMDF,ReleaseConstant.DM_RELEASE_CUSTOMER_CUBE,saveMode)
  } catch{
    case ex:Exception=>{
      logger.error(ex.getMessage(),ex)
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
