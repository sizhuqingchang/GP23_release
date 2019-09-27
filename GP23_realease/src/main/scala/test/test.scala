package test

import java.io.File


import org.apache.spark.sql.SparkSession

object test {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val format = "yyyy-MM-dd HH:mm:ss"
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()


    import sparkSession.sql
    import  sparkSession.implicits
    sparkSession.sql("use ods_release")
    sparkSession.sql("select * from ods_release.ods_01_release_session")


   sparkSession.stop()
  }

}
