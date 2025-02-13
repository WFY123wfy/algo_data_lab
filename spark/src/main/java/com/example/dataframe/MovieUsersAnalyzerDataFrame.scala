package com.example.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object MovieUsersAnalyzerDataFrame {
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

    println("通过DataFrame实现某部电影观看者中男性和女性不同年龄人数：")

    // Users数据格式化：在RDD的基础上增加数据的元数据信息
    val schemaForUsers = StructType(
      "UserID::Gender::Age::OccupationID::zip-code"
        .split("::")
        .map(column => StructField(column, StringType, true))
    )
    // 把每一条数据转换成以Row为单位的数据(1::F::1::10::48067)
    val usersRDDRows = usersRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim, line(4).trim))
    // 基于RDD创建DataFrame, 此时RDD有了元数据信息的描述
    val usersDataFrame = spark.createDataFrame(usersRDDRows, schemaForUsers)

    // ratings数据格式化：在RDD的基础上增加数据的元数据信息
    val schemaforratings = StructType("UserID::MovieID".split("::").map(column => StructField(column, StringType, true)))
      .add("Rating", DoubleType, true)
      .add("TimeStamp", StringType, true)
    val ratingsRDDRows = ratingsRDD
      .map(_.split("::"))
      .map(line => Row(line(0).trim, line(1).trim, line(2).trim.toDouble, line(3).trim))
    val ratingsDataFrame= spark.createDataFrame(ratingsRDDRows, schemaforratings)

    // movies数据格式化：在RDD的基础上增加数据的元数据信息
    val schemaformovies =StructType("MovieID::Title::Genres".split("::")
      .map(column => StructField(column,StringType,true)))
    val moviesRDDRows =moviesRDD
      .map(_.split("::"))
      .map(line=>Row(line(0).trim, line(1).trim, line(2).trim))
    val moviesDataFrame = spark.createDataFrame(moviesRDDRows, schemaformovies)

    // ================ 使用DataFrame操作 ================
    ratingsDataFrame
      .filter(s" MovieID = 1193")
      .join(usersDataFrame, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age") // 基于groupBy分组信息进行count统计操作
      .count()
      .show(10)

    // ================ 使用spark sql操作 ================
    // 把DataFrame注册为临时表
    ratingsDataFrame.createTempView("ratings")
    usersDataFrame.createTempView("users")
    // 写SQL语句并执行
    val sql_local = "SELECT " +
      "  Gender, Age, count(*) " +
      "from " +
      "   users u " +
      "join " +
      "   ratings as r " +
      "on " +
      "   u.UserID = r.UserID " +
      "where " +
      "   MovieID = 1193 " +
      "group by Gender, Age"
    spark.sql(sql_local).show()

    // DataFrame和RDD的混合编程
    ratingsDataFrame.select("MovieID","Rating")
      .groupBy("MovieID").avg("Rating")
      //这里直接使用DataFrame的rdd方法转到RDD里操作
      .rdd.map(row=>(row(1),(row(0),row(1))))
      .sortBy(_._1.toString.toDouble,false)
      .map(tuple=>tuple._2)
      .collect.take(10).foreach(println)

    spark.stop()
  }
}