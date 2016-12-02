package es

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * 统计不同渠道进件数量
  * Created by Michael on 2016/11/29.
  */
object SparkSQL_ES {

  /**
    * 使用模板类描述表元数据信息
    * 鹏远企业股东信息
    */
  case class gd_py_corp_sharehd_info(id:String,batch_seq_num:String,
                                     name:String,contributiveFund:String,
                                     contributivePercent:String,currency:String,
                                     contributiveDate:String,corp_basic_info_id:String,
                                     query_time:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custmer_Statistics").setMaster("local[2]")
    conf.set("es.nodes","192.168.20.128");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //RDD隐式转换成DataFrame
    import sqlContext.implicits._
    //读取本地文件
    val gd_py_corp_sharehd_infoDF = sc.textFile("C:/work/ideabench/SparkSQL/data/es/gd_py_corp_sharehd_info.txt")
      .map(_.split("\\t"))
      .map(d => gd_py_corp_sharehd_info(d(0), d(1), d(2), d(3), d(4), d(5), d(6), d(7), d(8)))
      .toDF()

    //注册表
    gd_py_corp_sharehd_infoDF.registerTempTable("gd_py_corp_sharehd_info")

    /**
      * 分渠道进件数量统计并按进件数量降序排列
      */
    val result = sqlContext
      .sql("select * from gd_py_corp_sharehd_info limit 10")
      .toDF()

    result.saveToEs("spark/gd_py_corp_sharehd_info")
  }

}

