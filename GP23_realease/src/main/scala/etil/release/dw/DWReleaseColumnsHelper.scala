package etil.release.dw

import scala.collection.mutable.ArrayBuffer

object DWReleaseColumnsHelper {

  def selectDWReleaseCustomerColumns():ArrayBuffer[String]={

    //定义字段
    var columns=new ArrayBuffer[String]()

    //增加字段信息
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("(cast(date_format(now(),'yyyy')as int) - cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{6})(\\\\d{4})',2) as int))as age")
    columns.+=("cast(regexp_extract(get_json_object(exts,'$.idcard'),'(\\\\d{16})(\\\\d{1})()',2) as int) % 2 as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  def selectDWReleaseExposureColumns():ArrayBuffer[String]={
    //定义字段
    var columns=new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  def selectDWReleaseRegisterColumns():ArrayBuffer[String]={
    //定义字段
    var columns=new ArrayBuffer[String]()
    columns.+=("get_json_object(exts,'$.user_register') as user_id")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  def selectDWReleaseClickColumns():ArrayBuffer[String]={
    //定义字段
    var columns=new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

}
