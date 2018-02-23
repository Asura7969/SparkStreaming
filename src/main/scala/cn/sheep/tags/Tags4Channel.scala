package cn.sheep.tags

import org.apache.commons.lang.StringUtils



import cn.sheep.beans.Logs


/**
  * Created by Administrator on 2017/8/16.
  */
object Tags4Channel extends  Tags{
  /**
    * 打标签的方法
    *打渠道的标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 0){
       val log = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotEmpty(log.channelid.toString)){
        map+=("CN".concat(log.channelid.toString) -> 1)
      }
    }
    map
  }
}
