package com.example.rdd

import org.apache.spark.sql.SparkSession

object SparkRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName).master("local").config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // 从内存中创建RDD
    val data = List(1, 2, 3, 4, 5)
    // parallelize：并行
    val rdd = spark.sparkContext.parallelize(data)
    // makeRDD方法在底层实现其实就是调用的rdd对象的parallelize方法
    val rdd1 = spark.sparkContext.makeRDD(data)

    // 转换操作：map
    val squaredRDD = rdd.map(x => x * x)

    // 行动操作：collect
    val squaredArray = squaredRDD.collect()

    // 打印结果
    println("Squared values: " + squaredArray.mkString(", "))
  }
}
