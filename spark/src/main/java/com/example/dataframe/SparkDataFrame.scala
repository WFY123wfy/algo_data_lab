package com.example.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object SparkDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local").config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // 创建包含名字和年龄的DataFrame
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
      ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

    // 将相同的名字分到一起，聚合年龄，并计算平均值
    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    // 展示最终的执行结果
    avgDF.show()
  }
}