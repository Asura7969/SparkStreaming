package cn.sheep.users

import java.util.UUID

import cn.sheep.beans.Logs
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2017/8/17.
  */
object DynamicallyGeneratingUserRelations {
  def main(args: Array[String]): Unit = {
//   if(args.length != 2){
//     println(
//       """
//         |cn.sheep.users.DynamicallyGeneratingUserRelations
//         |《参数一》 历史数据文件
          //《参数二》  followers.txt文件目录
//         |《参数三》 输出结果文件目录
//      """.stripMargin)
//     System.exit(0)
//   }

    //接收参数
   // val Array(inputdataPath,outputPath)=args
    //创建sparkconf对象
    val conf=new SparkConf()
    conf.setMaster("local")
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    //创建SparkCore对象
    val sc=new SparkContext(conf)
    /**
      * 这个地方一定要注意，要么进行cache，建议 最后就单独把这个结果进行持久化
      */
//    val baseData = sc.textFile(inputdataPath).map(line => {
//      val log = Logs.line2Logs(line)
//      val userIds = getUserIds(log)
//      (Math.abs(UUID.randomUUID().hashCode()), userIds)
//    }).filter(_._2.nonEmpty).cache()


   val testRDD=Array(Tuple2(1,Set("帅哥","李希沅")),Tuple2(2,Set("美男子","李希沅")),Tuple2(3,Set("李希沅")))
    val test = sc.parallelize(testRDD)


    test.flatMap( tuple =>{
      tuple._2.map( x =>(x,tuple._1.toString))  //帅哥，1   李希沅，1     美男子，2  李希沅，2    李希沅，3
    })
      .reduceByKey{
        (a,b) => a.concat(",").concat(b)
      }.map{
      t =>{
        val hashIds = t._2.split(",")
        var ships = new ArrayBuffer[(String,String)]()
        if(hashIds.length == 1){
          ships += Tuple2(hashIds(0),hashIds(0))
        }else{
          hashIds.map( x => ships +=Tuple2(hashIds(0),x))
        }
        ships.map( x =>x._1 +"\t"+x._2).toSeq.mkString("\n")
      }
    }
         // .saveAsTextFile("")
      .foreach(println(_))

    /**
      * 使用图计算
      */
    val graph = GraphLoader.edgeListFile(sc,"D:\\1704班\\spark\\第二十四天\\测试\\followers.txt")

    val cc = graph.connectedComponents().vertices
      cc.foreach(println(_))

    test.map( x=>(x._1.toLong,x._2)).join(cc)
        .map{
          case(hashId,(userIds,ccid)) =>(ccid,userIds)
        }.reduceByKey((_++_))
        .map( t => Math.abs(UUID.randomUUID().hashCode())+"\t"+t._2.mkString("\t"))
          .foreach( println(_))


    sc.stop()



  }

  /**
    * 获取用户id集合
    * @param log
    * @return
    */
  def getUserIds(log:Logs):Set[String]={
    var ids=Set[String]()
    if(log.imei.nonEmpty) ids++=Set("imei"+log.imei.toUpperCase())
    if(log.imeimd5.nonEmpty) ids++=Set("IMEIMD5"+log.imeimd5.toUpperCase())
    if(log.imeisha1.nonEmpty) ids ++=Set("IMEISHA1"+log.imeisha1.toUpperCase())

    if (log.androidid.nonEmpty) ids ++= Set("ANDROIDID:"+log.androidid.toUpperCase)
    if (log.androididmd5.nonEmpty) ids ++= Set("ANDROIDIDMD5:"+log.androididmd5.toUpperCase)
    if (log.androididsha1.nonEmpty) ids ++= Set("ANDROIDIDSHA1:"+log.androididsha1.toUpperCase)

    if (log.idfa.nonEmpty) ids ++= Set("IDFA:"+log.idfa.toUpperCase)
    if (log.idfamd5.nonEmpty) ids ++= Set("IDFAMD5:"+log.idfamd5.toUpperCase)
    if (log.idfasha1.nonEmpty) ids ++= Set("IDFASHA1:"+log.idfasha1.toUpperCase)

    if (log.mac.nonEmpty) ids ++= Set("MAC:"+log.mac.toUpperCase)
    if (log.macmd5.nonEmpty) ids ++= Set("MACMD5:"+log.macmd5.toUpperCase)
    if (log.macsha1.nonEmpty) ids ++= Set("MACSHA1:"+log.macsha1.toUpperCase)

    if (log.openudid.nonEmpty) ids ++= Set("OPENUDID:"+log.openudid.toUpperCase)
    if (log.openudidmd5.nonEmpty) ids ++= Set("OPENUDIDMD5:"+log.openudidmd5.toUpperCase)
    if (log.openudidsha1.nonEmpty) ids ++= Set("OPENUDIDSHA1:"+log.openudidsha1.toUpperCase)
    ids
  }

}
