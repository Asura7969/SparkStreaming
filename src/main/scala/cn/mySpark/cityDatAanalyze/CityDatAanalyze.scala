package cn.mySpark.cityDatAanalyze

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{SparkSession}

/**
  * Created by gongwenzhou on 2018/2/28.
  */
object CityDatAanalyze {
  def main(args: Array[String]): Unit = {
    if(args.length<3){
      println(
        """
          |cn.mySpark.cityDatAanalyze.CityDatAanalyze
          |<inputdataPath> 输入的parquet格式的文件目录
          |<resultJsonPath1> 输出路径1
          |<resultJsonPath2> 输出路径2
        """.stripMargin)
      System.exit(0)
    }
    val Array(inputdataPath,resultJsonPath1,resultJsonPath2)=args
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //注册自定义函数
    spark.udf.register("pn_cannot_cn",new PnCannotCn,DataTypes.StringType)
    spark.udf.register("random_perfix",new RandomPerfix,DataTypes.StringType)
    spark.udf.register("remove_perfix",new RemovePerfix,DataTypes.StringType)

    val inputDF = spark.read.parquet(inputdataPath)
    inputDF.createOrReplaceTempView("LOGS")

    val sql =
      """
        SELECT
              count(*) ct,
              provincename,
              cityname
        FROM LOGS
        GROUP BY
              provincename,
              cityname
        ORDER BY provincename
      """

    val _sql =
      """
        SELECT
              sum(t2.ct) count,
              pc
        FROM (
              SELECT
                    t1.ct,
                    remove_perfix(t1.perfix_pc) pc
              FROM (
                    SELECT
                          count(*) ct,
                          perfix_pc
                    FROM (
                          SELECT
                                random_perfix(pn_cannot_cn(provincename,cityname),10) perfix_pc
                          FROM LOGS
                    ) t1
                    GROUP BY t1.perfix_pc
              ) t2
        )GROUP BY t2.pc
      """


    //TopN
    val topN =
      """
        SELECT
              t1.ct,
              t1.pc
        FROM(
            SELECT
                  ct,
                  pc,
                  row_number over(partition by cityname order by ct desc) rn
            FROM cityDataCount
        ) t1
        WHERE t1.rn <= 3
      """

    val cityDataCountDF = spark.sql(sql)
    cityDataCountDF.write.json(resultJsonPath1)
    cityDataCountDF.createOrReplaceTempView("cityDataCount")
    //优化sql
    //spark.sql(_sql).write.json(resultJsonPath)
    //spark.sql(_sql).createOrReplaceTempView("cityDataCount")

    //topN
    spark.sql(topN).write.json(resultJsonPath2)
    spark.stop()

  }
}
