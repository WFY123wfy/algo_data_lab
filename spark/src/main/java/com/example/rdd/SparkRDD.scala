package com.example.rdd

import org.apache.spark.sql.SparkSession

object SparkRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local").config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // 创建一个 RDD
    val data = List(1, 2, 3, 4, 5)
    val rdd = spark.sparkContext.parallelize(data)

    // 转换操作：map
    val squaredRDD = rdd.map(x => x * x)

    // 行动操作：collect
    val squaredArray = squaredRDD.collect()

    // 打印结果
    println("Squared values: " + squaredArray.mkString(", "))
  }
}
