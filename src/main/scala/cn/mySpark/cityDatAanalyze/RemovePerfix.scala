package cn.mySpark.cityDatAanalyze

import org.apache.spark.sql.api.java.UDF1



/**
  * Created by gongwenzhou on 2018/2/28.
  */
class RemovePerfix extends UDF1[String,String]{
  /**
    * 删除随机前缀 9_江苏:常州
    * @param t1
    * @return
    */
  override def call(t1: String): String = {
    val split = t1.split("_")
    split(1)
  }
}
