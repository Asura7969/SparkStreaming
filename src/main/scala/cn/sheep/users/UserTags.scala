package cn.sheep.users

import cn.sheep.beans.Logs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/8/17.
  */
object UserTags {
  def main(args: Array[String]): Unit = {
//    if(args.length != 3){
//      println(
//        """
//          |cn.sheep.users.UserTags
//          |参数一 上下文标签合并的数据
//          |参数二  用户关系数据文件
//          |参数三  结果输出文件
//        """.stripMargin)
//    }
//    val Array(inputdataPath,usershipPath,resultOutput)=args
    //创建sparkconf对象
    val conf=new SparkConf()
    conf.setMaster("local")
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    //创建SparkCore对象
    val sc=new SparkContext(conf)

    /**
      * 获取到昨天根据上下文合并的数据 并解析
      */
    val ut = sc.textFile("D:\\1704班\\spark\\第二十四天\\测试\\上下文标签数据.txt")
      .map {
        line => {
          val fields = line.split("\t")
          val tags = fields.slice(1, fields.length).flatMap {
            tv => {
              var map = Map[String, Int]()
              val tkv = tv.split(":")
              map += (tkv(0) -> tkv(1).toInt)
              map
            }
          }

          (fields(0), tags.toList)
        }
      }
      //.foreach( t =>println(t._1 + "\t"+t._2))

    /**上面的输出结果：
      *李希沅	List((D00020005,2), (D00010003,2), (APP马上赚,2), (ZC上海市,2), (D00010001,2), (LC00,2), (ZP上海市,2))
       美男子	List((D00020005,2), (D00010003,2), (APP马上赚,1), (ZC上海市,2), (D00010001,2), (LC00,2), (ZP上海市,2))
      *
      */
    /**
      *
      */
    val us = sc.textFile("D:\\1704班\\spark\\第二十四天\\测试\\ship.txt")
      .flatMap {
        line => {
          val uIds = line.split("\t")
          uIds.slice(1, uIds.length).map(t => (t, uIds(0)))
        }
      }

        //.foreach( t=> println(t._1 + "\t"+t._2))

    /**
      *上一个步骤运行出来的结果
      * 帅哥	1328503827
       李希沅	1328503827
       美男子	1328503827
      *
      */
    us.join(ut).map{
      case(uid,(hashCode,tags)) =>(hashCode,tags)
    }.reduceByKey{
      case(list1,list2) =>{
        (list1++list2).groupBy(_._1).map{
          case(tk,stk) =>{
            val sum = stk.map(_._2).sum
            (tk,sum)
          }
        }.toList
      }
    }.foreach(println(_))


  }

}
