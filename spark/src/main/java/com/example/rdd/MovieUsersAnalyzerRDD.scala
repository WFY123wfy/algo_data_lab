package com.example.rdd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieUsersAnalyzerRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDD_Movie_Users_Analyzer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val dataPath = "./spark/ml-1m/"
    // ID、性别（F、M分别表示女性、男性）、年龄（使用7个年龄段标记）、职业和邮编
    val usersRDD = sc.textFile(dataPath + "users.dat")
    // 电影ID、电影名称和电影类型
    val moviesRDD = sc.textFile(dataPath + "movies.dat")
    // 用户ID、电影ID、评分（满分是5分）和时间戳
    val ratingsRDD = sc.textFile(dataPath + "ratings.dat")

    println("所有电影中平均分最高（口碑最好）的电影：")
    val movieInfo = moviesRDD.map(_.split("::")).map(x => (x(0), x(1))).cache() // 电影ID、电影名称
    val ratings = ratingsRDD.map(_.split("::")).map(x => (x(0), x(1), x(2))).cache() // 用户ID、电影ID、评分

    val moviesAndRatings = ratings.map(x => (x._2, (x._3.toDouble, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val avgRatings = moviesAndRatings.map(x => (x._1, x._2._1.toDouble / x._2._2))
    avgRatings.join(movieInfo)
      .map(item => (item._2._1, item._2._2))
      .sortByKey(false)
      .take(10)
      .foreach(record => println(record._2+"评分为："+record._1))

    spark.stop()
  }
}
