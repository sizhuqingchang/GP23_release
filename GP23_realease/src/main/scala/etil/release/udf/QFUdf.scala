package etil.release.udf

import com.qf.bigdata.release.util.CommonUtil

object QFUdf {
  def getRange(age:String):String={

    var tar=""
    try {
      tar= CommonUtil.getAgeRange(age)
    }catch {
      case ex:Exception=>{
        println(s"$ex")
      }
    }
    tar
  }
}
