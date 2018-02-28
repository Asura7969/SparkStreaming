package cn.mySpark.cityDatAanalyze

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

/**
  * Created by gongwenzhou on 2018/2/28.
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = DataTypes.createStructType(util.Arrays.asList(
    DataTypes.createStructField("carInfo",DataTypes.StringType,true)
  ))

  override def bufferSchema: StructType = DataTypes.createStructType(util.Arrays.asList(
    DataTypes.createStructField("bufferInfo",DataTypes.StringType,true)
  ))

  override def dataType: DataType = DataTypes.StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0,"")

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getString(0)+"1"
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0)+buffer2.getString(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getString(0)
}
