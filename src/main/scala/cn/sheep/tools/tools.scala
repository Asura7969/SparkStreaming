package cn.sheep.tools

import cn.sheep.beans.Logs
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/8/15.
  */
object tools {

  def main(args: Array[String]): Unit = {
    //1 判断参数个数
    if(args.length < 3){
      println(
        """
          |cn.sheep.tools <inputDataPath> <outputPath> <compressionCodec>
          |inputDataPath:输入日志文件路径
          |outputPath：输出文件路径
          |compressionCodec：指定的压缩格式
        """.stripMargin)
      System.exit(0)
    }
    //2 接收参数
    val Array(inputdatapath,outputdatapath,compressionCodec)=args
    //3 创建SparkConf对象
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Logs]))
    conf.set("spark.sql.parquet.compression.codec",compressionCodec)
    //4 创建SparkSession对象
    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()
    //5 开始去读取文件，转换成为DataFrame ，进行相对应的逻辑操作
    val rdd: RDD[Logs] = spark.sparkContext.textFile(inputdatapath)
      .map(line => Logs.line2Logs(line))
    //6 将结果文件存储起来
    spark.createDataFrame(rdd).write.parquet(outputdatapath)
    //7 释放资源
    spark.close()
  }

}
