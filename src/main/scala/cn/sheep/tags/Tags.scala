package cn.sheep.tags

import cn.sheep.beans.Logs

/**
  * Created by Administrator on 2017/8/16.
  打标签的方法
  */
trait Tags {
  /**
    * 打标签的方法
    * @param args
    * @return
    */
  def makeTags(args:Any*):Map[String,Int]

}
