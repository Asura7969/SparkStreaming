package cn.sheep.tags

import cn.sheep.beans.Logs

/**
  * Created by Administrator on 2017/8/16.
  */
object Tags4Device extends  Tags{
  /**
    * 打标签的方法
    * 设备标签
    * 1）操作系统
    * 2）联网方式
    * 3）运营商
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 1){
      val log = args(0).asInstanceOf[Logs]
      val devicedict = args(1).asInstanceOf[Map[String,String]]
      //操作系统
     val os=devicedict.getOrElse(log.client.toString,devicedict.get("4").get)
       map +=(os ->1)
      //联网方式
      val network = devicedict.getOrElse(log.networkmannername.toString,devicedict.get("NETWORKOTHER").get)
      map +=(network -> 1)
      // /运营商
      val isp = devicedict.getOrElse(log.ispname,devicedict.get("OPERATOROTHER").get)
      map +=(isp -> 1)
    }
    map
  }
}
