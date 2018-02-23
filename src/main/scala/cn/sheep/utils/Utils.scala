package cn.sheep.utils

import java.util.regex.Pattern

import org.apache.commons.lang.StringUtils

/**
  * Created by Administrator on 2017/8/15.
  */
object Utils {
  /**
    * 将普通的字符串转换为Int类型
    * @param s
    * @return
    */
  def parseInt(s:String):Int={
    try {
      s.isEmpty match {
        case true => 0
        case false => s.toInt
      }
    } catch {
      case _:Exception => 0
    }
  }

  /**
    * 将普通的字符串转换为double类型
    * @param s
    * @return
    */
  def parseDouble(s:String):Double={
    try {
      if (StringUtils.isNotEmpty(s)) {
        s.toDouble
      } else {
        0.0
      }
    } catch {
      case _:Exception => 0.0
    }
  }
  //2017-09-09 09:09:09
  //fmtDate 20170909
  //fmtHour  09
  /**
    *从日期里面截取天
    * @param s 2017-09-09 09:09:09
    * @return  20170909
    */
  def fmtDate(s:String): Option[String]={
    if(StringUtils.isNotEmpty(s)){
      val fields = s.split(" ")
      if(fields.length > 1){
        Some(fields(0).replace("-",""))
      }else{
        None
      }
    }else{
      None
    }
  }

  /**
    *
    * @param s 2017-07-08 09:10:11
    * @return 09
    */
  def fmtHour(s:String):Option[String]={
    if(StringUtils.isNotEmpty(s)){
        val fields = s.split(" ")
      if(fields.length > 1){
        Some(fields(1).substring(0,2))
      }else{
        None
      }

    }else{
      None
    }
  }

  /**
    * 将imei格式化成标准的imei
    * @param imei
    * @return
    */
  def formatIMEID(imei: String): String = {
    var imeiId = ""
    val imeiNum = if (Utils.isAllNumber(imei) && (imei.length == 15 || imei.length == 14)) imei else ""
    if (imeiNum.length == 14) {
      imeiId = imeiNum + getMeid15(imei)
    } else if (imeiNum.length == 15) {
      imeiId = imeiNum
    }
    val IMEILet = if (Utils.isContainLet(imei) && (imei.length == 15 || imei.length == 14)) imei else ""
    if (IMEILet.length == 14) {
      imeiId = formatMeid(IMEILet).toUpperCase
    } else if (IMEILet.length == 15) {
      imeiId = IMEILet
    }
    imeiId
  }

  /**
    * 判断是否全部是数字
    *
    * @param str
    * @return boolean
    */
  private def isAllNumber(str: String) = Pattern.compile("[0-9]*").matcher(str).matches()

  /**
    * 判断是否含有数字
    *
    * @param str
    * @return boolean
    */
  private def isContainNumber(str: String) = Pattern.compile("[0-9]*").matcher(str).find()

  /**
    * 判断是否仅由字母和数字组成，包括全是字母以及全是数字
    *
    * @param str
    * @return boolean
    */
  private def isAllLetAndNum(str: String) = Pattern.compile("[a-zA-Z0-9]*").matcher(str).matches()

  /**
    * 判断是否全部是字母
    *
    * @param str
    * @return boolean
    */
  private def isAllLet(str: String) = Pattern.compile("[a-zA-Z]+").matcher(str).matches()

  /**
    * 判断是否含有字母
    *
    * @param str
    * @return boolean
    */
  private def isContainLet(str: String) = Pattern.compile("[a-zA-Z]+").matcher(str).find()


  /**
    * 判断是十六进制
    *
    * @param str
    * @return boolean
    */
  private def isHexadecimal(str: String) = Pattern.compile("[a-fA-F0-9]*").matcher(str).matches()

  /**
    * 格式化MEID
    * 因为MEID格式不统一，长度有14位和16位的，所以，为了统一，将14位和16位的MEID，统一设置为15位的 设置格式：
    * 如果MEID长度为14位，那么直接得到第15位，如果MEID长度为16位，那么直接在根据后14位得到第15位
    * 如果MEID长度为其他长度，那么直接返回原值
    *
    * @param meid
    * @return
    */
  private def formatMeid(meid: String): String = {
    val dxml: Int = meid.length
    if (dxml != 14 && dxml != 16) {
      return meid
    }
    var meidRes: String = ""
    if (dxml == 14 && isHexadecimal(meid)) {
      meidRes = meid + getMeid15(meid)
    } else {
      meidRes = meid
    }
    if (dxml == 16) {
      meidRes = meid.substring(2) + getMeid15(meid.substring(2))
    }
    meidRes
  }

  /**
    * 根据MEID的前14位，得到第15位的校验位
    * MEID校验码算法：
    * (1).将偶数位数字分别乘以2，分别计算个位数和十位数之和，注意是16进制数
    * (2).将奇数位数字相加，再加上上一步算得的值
    * (3).如果得出的数个位是0则校验位为0，否则为10(这里的10是16进制)减去个位数
    * 如：AF 01 23 45 0A BC DE 偶数位乘以2得到F*2=1E 1*2=02 3*2=06 5*2=0A A*2=14 C*2=18 E*2=1C,
    * 计算奇数位数字之和和偶数位个位十位之和，得到 A+(1+E)+0+2+2+6+4+A+0+(1+4)+B+(1+8)+D+(1+C)=64
    * 校验位 10-4 = C
    *
    * @param meid
    * @return
    */
  private def getMeid15(meid: String): String = {
    if (meid.length == 14) {
      val myStr = Array("a", "b", "c", "d", "e", "f")
      var sum = 0
      for (i <- 0 to meid.length - 1) {
        var param = meid.substring(i, i + 1)

        for (j <- myStr.indices) {
          if (param.equalsIgnoreCase(myStr(j))) {
            param = "1" + String.valueOf(j)
          }
        }

        if (i % 2 == 0) {
          sum = sum + param.toInt
        }
        else {
          sum = sum + 2 * param.toInt % 16
          sum = sum + 2 * param.toInt / 16
        }
      }

      if (sum % 16 == 0) {
        "0"
      }
      else {
        var result = 16 - sum % 16
        if (result > 9) {
          result += 65 - 10
        }
        result + ""
      }
    }
    else {
      ""
    }
  }


  def main(args: Array[String]): Unit = {
    val date = fmtDate("2017-07-08 09:10:11").get
    val hour = fmtHour("2017-07-08 09:10:11").get
    println(date)
    println(hour)
  }

}
