package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SparkHelper {

  private val logger: Logger = LoggerFactory.getLogger(SparkHelper.getClass)


  //创建sparkSession
  def createSpark(conf: SparkConf): SparkSession = {

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()


    spark
  }


  //读取hive表数据
  def readTable(spark: SparkSession, tableName: String, columnsName: mutable.Seq[String]): DataFrame = {

    import spark.implicits._

    //读取数据
    val tableDF: DataFrame = spark.read.table(tableName)
      .selectExpr(columnsName: _*)
    tableDF
  }

  //数据写入hive表
  def writeTableData(sourceDF: DataFrame, table: String, saveMode: SaveMode): Unit = {
    // 写入表
    sourceDF.write.mode(saveMode).insertInto(table)
  }

  //参数校验
  def rangeDates(begin: String, end: String): Seq[String] = {
    val bdp_days = new ArrayBuffer[String]()

    val bpd_begin_day: String = DateUtil.dateFromat4String(begin, "yyyy-MM-dd")
    val bpd_end_day: String = DateUtil.dateFromat4String(end, "yyyy-MM-dd")


    if (bpd_begin_day.equals(bpd_end_day)) {
      bdp_days.+=(bpd_begin_day)
    } else {
      var cday = bpd_begin_day
      while (cday != bpd_end_day) {
        bdp_days.+=(cday)
        // 让初始时间累加，以天为单位
        val pday = DateUtil.dateFromat4StringDiff(cday, 1, "yyyy-MM-dd")
        cday = pday
      }

    }
    bdp_days
  }
}
