package constant

import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

object ReleaseConstant {

   val DEF_STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_AND_DISK
   val DEF_PARTITION = "bdp_day"
   val DEF_REPARTITION_NUMBER = 4

   //维度列
   val COL_RELEASE_SESSION_STATUS:String = "release_status"

   val COL_RELEASE_SESSION_DEVICE_NUM = "device_num"
   val COL_RELEASE_DEVICE_TYPE = "device_type"
   val COL_RELEASE_SOURCES = "sources"
   val COL_RELEASE_CHANNELS = "channels"
   val COL_RELEASE_AGE = "age"
   val COL_RELEASE_AGE_RANGE = "age_range"
   val COL_RELEASE_GENDER = "gender"
   val COL_RELEASE_AREA_CODE = "area_code"
   val COL_RELEASE_USER_COUNT = "user_count"
   val COL_RELEASE_TOTAL_COUNT = "total_count"
   val COL_RELEASE_EXPOSURE_COUNT = "exposure_count"
   val COL_RELEASE_EXPOSURE_RATES = "exposure_rates"
   val COL_RELEASE_AID = "aid"
   val COL_RELEASE_USER_ID="user_id"


   // ods================================
   val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"
   val ODS_RELEASE_USER = "ods_release.ods_release_users"

   //dw==================================
   val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"
   val DW_RELEASE_EXPOSURE = "dw_release.dw_release_exposure"
   val DW_RELEASE_REGISTER = "dw_release.dw_release_register_users"
   val DW_RELEASE_CLICK = "dw_release.dw_release_click"

   //dm==================================
   val DM_RELEASE_CUSTOMER_SOURCES = "dm_release.dm_customer_sources"
   val DM_RELEASE_CUSTOMER_CUBE = "dm_release.dm_customer_cube"
   val DM_RELEASE_EXPOSURE_SOURCE = "dm_release.dm_exposure_sources"
   val DM_RELEASE_EXPOSURE_CUBE = "dm_release.dm_exposure_cube"
   val DM_RELEASE_REGISTER_USER = "dm_release.dm_register_users"
   val DM_RELEASE_CLICK_CUBE = "dm_release.dm_release_click_cube"





}
