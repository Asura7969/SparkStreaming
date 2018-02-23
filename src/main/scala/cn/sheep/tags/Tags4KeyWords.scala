package cn.sheep.tags

import cn.sheep.beans.Logs
import org.apache.commons.lang.StringUtils

/**
  * Created by Administrator on 2017/8/16.
  */
object Tags4KeyWords  extends  Tags{
  /**
    * 打标签的方法
    *给关键字打标签
    * @param args
    * @return
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map=Map[String,Int]()
    if(args.length > 0){
       val log = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotEmpty(log.keywords)){
        val fields = log.keywords.split("\\|")
        fields.filter( e =>{
          e.length >=3 && e.length <= 8
        }).map( word => map+=("K"+word.replace(":","") ->1))
      }
    }
    map
  }
}
