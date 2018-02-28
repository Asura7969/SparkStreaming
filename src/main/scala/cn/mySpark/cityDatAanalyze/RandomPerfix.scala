package cn.mySpark.cityDatAanalyze

import org.apache.spark.sql.api.java.UDF2

import scala.util.Random


/**
  * Created by gongwenzhou on 2018/2/28.
  */
class RandomPerfix extends UDF2[String,Int,String]{
  /**
    * 添加随机前缀
    * @param t1
    * @param t2
    * @return
    */
  override def call(t1: String,t2:Int): String = {
    val random = new Random
    t1 + "_" + random.nextInt(t2)
  }
}
