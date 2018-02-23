package cn.sheep.tags

import cn.sheep.beans.Logs

/**
  * Created by Administrator on 2017/8/16.
  */
object Tags4Area extends  Tags{
  /**
    * 打标签的方法
    *区域标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 0){
       val log = args(0).asInstanceOf[Logs]
      if(log.provincename.nonEmpty){
        map += ("ZP"+log.provincename -> 1)
      }
      if(log.cityname.nonEmpty){
        map += ("ZC"+log.cityname -> 1)
      }
    }
    map
  }
}
