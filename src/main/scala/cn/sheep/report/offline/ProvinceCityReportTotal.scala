package cn.sheep.report.offline

import cn.sheep.beans.Logs
import cn.sheep.tools.ReportUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/8/15.
  */
object ProvinceCityReportTotal {
  def main(args: Array[String]): Unit = {
    //判断参数个数
    if(args.length < 3){
      println(
        """
          |cn.sheep.report.offline.ProvinceCityReportTotal
          |<inputdataPath> 输入数据目录
          |<provinceOutputPath> 省份结果存储目录
          |<cityOutputPath> 城市结果存储目录
        """.stripMargin)
      System.exit(0)
    }
    //接收参数
    val Array(inputdatapath,provinceoutputpath,cityoutputpath)=args
    //添加配置选项
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    //创建一个sparkContext对象
    val sc = new SparkContext(conf)
    //逻辑代码处理
    val rdd = sc.textFile(inputdatapath).map(line => {
      val log = Logs.line2Logs(line)
      val adRequest = ReportUtils.calculateAdRequest(log)
      val adResponse = ReportUtils.calculateResponse(log)
      val adShowclick = ReportUtils.calculateShowClick(log)
      val adCost = ReportUtils.calculateAdCost(log)
      (log.provincename, log.cityname, adRequest ++ adResponse ++ adShowclick ++ adCost)

    })

    //计算 各省份的指标  存储结果
    rdd.map{
      case(p,c,list) =>{
        (p,list)
      }
    }.reduceByKey{
      case(list1,list2) =>{
        list1.zip(list2).map{
          case(v1,v2) => v1+v2
        }
      }
    }.map( t => t._1 + "\t"+t._2.mkString(",")).saveAsTextFile(provinceoutputpath)
    //计算 各省份的城市的指标  存储结果
    rdd.map{
      case(p,c,list) =>{
        (p+"_"+c,list)
      }
    }.reduceByKey{
      case(list1,list2) =>{
        list1.zip(list2).map{
          case(v1,v2) => v1+v2
        }
      }
    }.map( t => t._1 + "\t"+t._2.mkString(",")).saveAsTextFile(cityoutputpath)
    //释放资源

    sc.stop()

  }

}
