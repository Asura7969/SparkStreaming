package cn.mySpark.cityDatAanalyze

import org.apache.spark.sql.api.java.UDF2

/**
  * Created by gongwenzhou on 2018/2/28.
  */
class PnCannotCn extends UDF2[String,String,String]{
  /**
    *
    * @param t1 省份名称
    * @param t2 城市名称
    * @return 省份名称_城市名称
    */
  override def call(t1: String, t2: String): String = t1+":"+t2
}
