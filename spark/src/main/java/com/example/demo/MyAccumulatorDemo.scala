package com.example.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MyAccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // 内置累加器
    val accum = sc.longAccumulator("My Accumulator")
    sc.parallelize(Array(1,2,3,4,5,6,7,8)).foreach(x=>accum.add(1))
    println(accum.value)

    // 自定义累加器
    val MyAccum = new MyAccumulator()
    // 向SparkContext注册累加器
    sc.register(MyAccum)
    sc.parallelize(Array("a", "b", "c", "d", "e", "f")).foreach(x => MyAccum.add(x))
    println(MyAccum.value)
    // 结果显示的ArrayBuffer里的值顺序是不固定的，取决于各个Executor的值到达Driver的顺序
  }
}