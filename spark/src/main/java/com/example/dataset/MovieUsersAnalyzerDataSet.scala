package com.example.dataset

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MovieUsersAnalyzerDataSet {
  // 创建case User来封装用户数据
  case class User(UserId:String, Gender:String, Age:String, OccupationID:String, zipCode:String)
  // 创建case Rating来封装用户评分数据
  case class Rating(UserID:String, MovieID:String, Rating:Double, Timestamp:String)

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

    import spark.implicits._
    // 创建用户DataSet
    val usersForDSRDD = usersRDD
      .map(_.split("::"))
      .map(line => User(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    val usersDataSet = spark.createDataset(usersForDSRDD)
    //  usersDataSet.show(10)

    // 创建用户评分DataSet
    val ratingsForDSRDD = ratingsRDD
      .map(_.split("::"))
      .map(line => Rating(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataSet = spark.createDataset(ratingsForDSRDD)
    //  ratingsDataSet.show(10)

    // 数据处理
    ratingsDataSet.filter(s" MovieID = 1193")
      .join(usersDataSet, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .orderBy($"Gender".desc, $"Age")
      .show(10)

    spark.stop()
  }
}