package sparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * Created by gongwenzhou on 2018/2/23.
  */
object SparkSQLTest {

  case class Person(var name:String,var age:Int)

  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local")
    val sparkSession = SparkSession.builder()
      .appName(s"${this.getClass.getName}")
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    /**
      * 获取Dataframe
      */
    val peopleDF: DataFrame = sparkSession.read.json("D:\\people.json")
    peopleDF.show()

    /**
      * 获取RDD
      */
    val inputRDD: RDD[String] = sparkSession.sparkContext.textFile("D:\\people.txt")
    inputRDD
      .map(_.split(","))
      .map(p=>Person(p(0),p(1).trim.toInt))
      .filter(_.age<20).foreach(println)
    /**
      * RDD转DataFrame
      */
    import sparkSession.implicits._

    val peopleDF2: DataFrame = inputRDD.toDF()
    peopleDF2.createOrReplaceTempView("people")

    import sparkSession.sql
    sql("select * from people").show()
    val peopleDataset: Dataset[Person] = peopleDF2.as[Person]

    //Dataset DataFrame操作
    peopleDataset.createOrReplaceTempView("people2")
    sql("select * from people2").show()

    //Dataset RDD操作
    peopleDataset.filter(_.age<20).show()

    sparkSession.close()

  }


}
