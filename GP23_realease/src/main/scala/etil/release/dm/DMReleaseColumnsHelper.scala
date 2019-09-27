package etil.release.dm

import scala.collection.mutable.ArrayBuffer

object DMReleaseColumnsHelper {

  /**
    * 目标客户
    */
  def selectDWReleaseCustomerColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("idcard")
    columns.+=("age")
    columns.+=("getAgeRange(age) as age_range")
    columns.+=("gender")
    columns.+=("aid")
    columns.+=("area_code")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 目标用户渠道指标列
    */
  def selectDMCustomerSourceColumns():ArrayBuffer[String] ={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 目标客户多维度分析统计
    */
  def selectDMCustomerCudeColumns():ArrayBuffer[String]={
    val columns = new ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("user_count")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }


  /**
    * 获取曝光
    */
  def selectDWReleaseExposureColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 渠道曝光统计
    */

  def selectDMExposureSourceColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }
  /**
    * 曝光多维统计
    */

  def selectDMExposureCubeColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("device_type")
    columns.+=("age_range")
    columns.+=("gender")
    columns.+=("area_code")
    columns.+=("exposure_count")
    columns.+=("exposure_rates")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 点击主题
    */
  def selectDWClickColumns():ArrayBuffer[String]={
  val columns=ArrayBuffer[String]()
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 按类型统计分析
    */
  def selectDMClickColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("age_range")
    columns.+=("aid")
    columns.+=("total_count")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 注册
    */
  def selectDWRegisterColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("user_id")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("bdp_day")
    columns
  }

  /**
    *新增用户
    */
  def selectDMRegisterColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("platform")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("new_user_count")
    columns.+=("bdp_day")
    columns
  }

  def selectOdsRegisteredColumns():ArrayBuffer[String]={
    val columns=ArrayBuffer[String]()
    columns.+=("user_id")
    columns.+=("user_pass")
    columns.+=("ctime")
    columns
  }

}
