package utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object DateUtil {

  def dateFromat4String(date:String,formater:String):String={

    if(date == null)
      return null

    val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(formater)
    val datetime: LocalDate = LocalDate.parse(date,timeFormatter)

    // 校验时间参数
    datetime.format(DateTimeFormatter.ofPattern(formater))
  }

  def dateFromat4StringDiff(date:String,diff:Long,formater:String):String = {

    if(date == null)
      return null

    val timeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(formater)
    val datetime: LocalDate = LocalDate.parse(date,timeFormatter)

    // 处理天的累加
    val resultDateTime = datetime.plusDays(diff)

    resultDateTime.format(DateTimeFormatter.ofPattern(formater))
  }
}
