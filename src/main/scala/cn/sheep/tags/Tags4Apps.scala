package cn.sheep.tags

import cn.sheep.beans.Logs
import org.apache.commons.lang.StringUtils

/**
  * Created by Administrator on 2017/8/16.
  */
object Tags4Apps extends  Tags{
  /**
    * 打标签的方法
    *APP标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
     var map=Map[String,Int]()
    if(args.length > 1){
       val log = args(0).asInstanceOf[Logs]
       val appdict = args(1).asInstanceOf[Map[String,String]]
      val appName = appdict.getOrElse(log.appid,log.appname)
      if(StringUtils.isNotEmpty(appName)){
        map += ("APP"+appName -> 1)
      }
    }
    map
  }
}
