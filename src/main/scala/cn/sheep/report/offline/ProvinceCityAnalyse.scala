package cn.sheep.report.offline

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/15.
  *  统计各省市数据量分布情况
  *  输入文件：parquet文件
  *  输出文件：json格式
  */
object ProvinceCityAnalyse {
  def main(args: Array[String]): Unit = {
     //1 判断参数个数
    if(args.length < 2){
      println(
        """
          |cn.sheep.report.offline.ProvinceCityAnalyse
          |<inputdataPath> 输入的parquet格式的文件目录
          |<resultJsonPath>输出的结果文件目录
        """.stripMargin)
      System.exit(0)
    }
    //2 接收参数
    val Array(inputdataPath,outputdataPath)=args
    //3 添加配置参数
    val conf=new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName}")
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    //4 读取文件，做逻辑代码开发
    val df = spark.read.parquet(inputdataPath)
    df.createOrReplaceTempView("logs")
    val sql=
      """
         select count(*) ct,provincename,cityname
               from logs
               group by
                       provincename,cityname
               order by provincename
      """

    //5 存储结果文件
    spark.sql(sql).write.json(outputdataPath)
    //6 释放资源
    spark.stop()
  }

}
