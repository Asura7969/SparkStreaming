package cn.sheep.report.offline

import cn.sheep.beans.Logs
import cn.sheep.tools.ReportUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/8/15.
  */
object AppReportTotal {
  def main(args: Array[String]): Unit = {
    if(args.length <3){
      println(
        """
          |cn.sheep.report.offline.AppReportTotal
          |参数一：输入的历史日志文件
          |参数二：app 映射文件
          |参数三：结果存储目录
        """.stripMargin)
      System.exit(0)
    }
    val Array(inputdataPath,appmappingPath,appoutputPath)=args

    //添加配置选项
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    //创建一个sparkContext对象
    val sc = new SparkContext(conf)

    //先获取到映射文件
    val appMap: Map[String, String] = sc.textFile(appmappingPath).map(line => {
      import scala.collection.mutable.Map
      var map = Map[String, String]()
      val fields = line.split("\t")
      map += (fields(4) -> fields(1))
      map
    }).collect()
      .flatten
      .toMap

      val broadcastRDD = sc.broadcast(appMap)

    //读取日志数据
    sc.textFile(inputdataPath).map( line =>{
      val log = Logs.line2Logs(line)
      val adRequest = ReportUtils.calculateAdRequest(log)
      val adresponse = ReportUtils.calculateResponse(log)
      val adShowClick = ReportUtils.calculateShowClick(log)
      val adCost = ReportUtils.calculateAdCost(log)
      (broadcastRDD.value.getOrElse(log.appid,log.appname),adRequest ++ adresponse ++ adShowClick ++ adCost)
    }).filter(_._1.nonEmpty)
       .reduceByKey{
         case(list1,list2) => {
           list1.zip(list2).map{
             case(x,y) => x+y
           }
         }
       }.map( t => t._1+"\t"+t._2.mkString(",")).saveAsTextFile(appoutputPath)

    sc.stop()

  }

}
