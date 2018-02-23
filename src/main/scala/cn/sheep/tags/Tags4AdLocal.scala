package cn.sheep.tags

import cn.sheep.beans.Logs

/**
  * Created by Administrator on 2017/8/16.
  */
object Tags4AdLocal extends  Tags{
  /**
    * 打标签的方法
    * 给广告位置打标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
     if(args.length > 0){
       val log = args(0).asInstanceOf[Logs]
       log.adspacetype match {
         case x if x < 10 => map +=("LC0".concat(x.toString) -> 1)
         case x if x > 9 => map +=("LC".concat(x.toString) -> 1)
       }
     }
    map
  }
}
