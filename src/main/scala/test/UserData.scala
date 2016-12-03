package test

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Michael on 2016/12/3.
  */
object UserData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserData").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //RDD隐式转换成DataFrame
    import sqlContext.implicits._
    //读取本地文件
    val userData = Array(
      "2016-3-27,001,http://spark.apache.org/,1000",
      "2016-3-27,001,http://hadoop.apache.org/,1001",
      "2016-3-27,002,http://fink.apache.org/,1002",
      "2016-3-28,003,http://kafka.apache.org/,1020",
      "2016-3-28,004,http://spark.apache.org/,1010",
      "2016-3-28,002,http://hive.apache.org/,1200",
      "2016-3-28,001,http://parquet.apache.org/,1500",
      "2016-3-28,001,http://spark.apache.org/,1800"
    )
    val userDataRDD = sc.parallelize(userData)  //生成DD分布式集合对象
    val userDataRDDRow = userDataRDD.map(
      row => {
        val splited = row.split(",") ;
        Row(splited(0),splited(1).toInt,splited(2),splited(3).toInt)
      }
    )
    val structTypes = StructType(Array(
      StructField("time", StringType, true),
      StructField("id", IntegerType, true),
      StructField("url", StringType, true),
      StructField("amount", IntegerType, true)
    ))
    val userDataDF = sqlContext.createDataFrame(userDataRDDRow,structTypes)
    userDataDF.registerTempTable("userData")
    sqlContext.sql("select sum(amount) from userData group by time").show()

/*

    userDataDF.groupBy("time").agg('time, countDistinct('id)).map(row=>Row(row(1),row(2))).collect.foreach(println)
    userDataDF.groupBy("time").agg('time, sum('amount)).show()
*/

  }


}
