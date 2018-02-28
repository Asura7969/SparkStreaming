package cn.sheep.utils

/**
  * Created by gongwenzhou on 2018/2/28.
  */
object MyStringUtils {

  /**
    * 从拼接的字符串中提取字段值
    * @param str 字符串
    * @param delimiter 分隔符
    * @param field  字段
    * @return 字段值
    */
  def getFieldFromConcatString(str: String,
                               delimiter: String, field: String):String={

    val split = str.split(delimiter)
    var value:String = null
    for(index <- split){
      if(index.split("=")(0).contains(field)){
        value = index.split("=")(1)
      }
    }
    value
  }

  /**
    * 从拼接的字符串中给字段设置值
    * @param str           字符串
    * @param delimiter     分隔符
    * @param field         字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    *         name=zhangsan123
    *         age=18
    *         name=zhangsan|age=18
    */
  def setFieldInConcatString(str: String,
                             delimiter: String, field: String, newFieldValue: String): Unit ={



  }

  def main(args: Array[String]): Unit = {
    println(getFieldFromConcatString("aa=1|bb=2","\\|","aa"))
  }
}
