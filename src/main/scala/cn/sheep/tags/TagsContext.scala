package cn.sheep.tags

import cn.sheep.beans.Logs
import cn.sheep.utils.Utils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/8/16.
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //判断参数个数
    if(args.length < 4){
      println(
        """
          |cn.sheep.tags.TagsContext
          |<参数一>  原始日志文件
          |<参数二>  app映射文件
          |<参数三>  设备映射文件
          |<参数四>  标签结果文件存储
         """.stripMargin)
      System.exit(0)
    }
    //接收参数
    val Array(inputdataPath,appfilePath,devicefilePath,tagsoutputPath)=args
    //创建sparkconf对象
    val conf=new SparkConf()
    conf.setMaster("local")
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    //创建SparkCore对象
    val sc=new SparkContext(conf)
    //接收app映射文件，并转化成为Map结构  并广播出去
    val appdict = sc.textFile(appfilePath).map {
      line => {
        val fields = line.split("\t", line.length)
        var map = Map[String, String]()
        if (fields.length > 4) {
          map += (fields(4) -> fields(1))
        }
        map
      }
    }.collect().flatten.toMap
    val appdictbroadcast = sc.broadcast(appdict)
    //接收device映射，并解析成为Map结构  并广播出去
    val devicedict = sc.textFile(devicefilePath).flatMap {
      line => {
        val fields = line.split("\t", line.length)
        var map = Map[String, String]()
        map += (fields(0) -> fields(1))
        map
      }
    }.collect().toMap

   val devicebroadcast = sc.broadcast(devicedict)
    //读取日志，打标签
    sc.textFile(inputdataPath).map{
      line =>{
        val log = Logs.line2Logs(line)
       val areaTag = Tags4Area.makeTags(log)
        val deviceTag = Tags4Device.makeTags(log,devicebroadcast.value)
        val appTag = Tags4Apps.makeTags(log,appdictbroadcast.value)
        val localTag = Tags4AdLocal.makeTags(log)
        val keywordTag = Tags4KeyWords.makeTags(log)
        val channelTag = Tags4Channel.makeTags(log)

        val uid = getNotEmptyID(log).getOrElse("")
         println(uid)
        (uid,(areaTag ++ deviceTag ++ appTag ++  localTag ++ keywordTag ++ channelTag).toList)

      }
    }.filter( !_._1.toString.equals(""))
      .reduceByKey{
        case(list1,list2) =>{
          (list1++list2).groupBy{
            case(k,v) => k
          }.map{
            case(k1,v1) => (k1,v1.map(t => t._2).sum)
          }.toList
        }
          //userid
      }.map( t => t._1+"\t"+t._2.map( x=> x._1 + ":"+x._2).mkString("\t"))
      .foreach(println(_))
    //释放资源
    sc.stop()
  }


  // 获取用户唯一不为空的ID
  def getNotEmptyID(log: Logs): Option[String] = {
    log match {
      case v if v.imei.nonEmpty => Some("IMEI:" + Utils.formatIMEID(v.imei))
      case v if v.imeimd5.nonEmpty => Some("IMEIMD5:" + v.imeimd5.toUpperCase)
      case v if v.imeisha1.nonEmpty => Some("IMEISHA1:" + v.imeisha1.toUpperCase)

      case v if v.androidid.nonEmpty => Some("ANDROIDID:" + v.androidid.toUpperCase)
      case v if v.androididmd5.nonEmpty => Some("ANDROIDIDMD5:" + v.androididmd5.toUpperCase)
      case v if v.androididsha1.nonEmpty => Some("ANDROIDIDSHA1:" + v.androididsha1.toUpperCase)

      case v if v.mac.nonEmpty => Some("MAC:" + v.mac.replaceAll(":|-", "").toUpperCase)
      case v if v.macmd5.nonEmpty => Some("MACMD5:" + v.macmd5.toUpperCase)
      case v if v.macsha1.nonEmpty => Some("MACSHA1:" + v.macsha1.toUpperCase)

      case v if v.idfa.nonEmpty => Some("IDFA:" + v.idfa.replaceAll(":|-", "").toUpperCase)
      case v if v.idfamd5.nonEmpty => Some("IDFAMD5:" + v.idfamd5.toUpperCase)
      case v if v.idfasha1.nonEmpty => Some("IDFASHA1:" + v.idfasha1.toUpperCase)

      case v if v.openudid.nonEmpty => Some("OPENUDID:" + v.openudid.toUpperCase)
      case v if v.openudidmd5.nonEmpty => Some("OPENDUIDMD5:" + v.openudidmd5.toUpperCase)
      case v if v.openudidsha1.nonEmpty => Some("OPENUDIDSHA1:" + v.openudidsha1.toUpperCase)

      case _ => None
    }

  }

}
