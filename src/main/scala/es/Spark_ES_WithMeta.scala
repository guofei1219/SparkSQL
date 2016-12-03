package es

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import utils.PropertiesUtils

import scala.collection.immutable
import scala.collection.mutable.ListBuffer

/**
  * 统计不同渠道进件数量
  * Created by Michael on 2016/11/29.
  */
object Spark_ES_WithMeta {

  val buffer = new ListBuffer[Tuple2[String,immutable.Map[String,String]]]
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custmer_Statistics").setMaster("local[2]")
    conf.set("es.nodes","rmhadoop01,rmhadoop02,rmhadoop03");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    //读取本地文件

    val result = sc.textFile("C:/work/ideabench/SparkSQL/data/es/gd_py_corp_sharehd_info.txt")
      .map(_.split("\\t"))
      .foreach(d =>{
          if(PropertiesUtils.getStringByKey("gd_py_corp_sharehd_info").equals("one2many")){

            val map = Map("id"->d(0),
              "batch_seq_num"->d(1),
              "name"->d(2),
              "contributiveFund"->d(3),
              "contributivePercent"->d(4),
              "currency"->d(5),
              "contributiveDate"->d(6),
              "corp_basic_info_id"->d(7),
              "query_time"->d(8)
            )

            buffer.append((d(0),map))
            //buffer

          }else if(PropertiesUtils.getStringByKey("gd_py_corp_sharehd_info").equals("one2one")){
            //Map(d(1) ->gd_py_corp_sharehd_info(d(0), d(1), d(2), d(3), d(4), d(5), d(6), d(7), d(8)))
          }

      } )

    sc.makeRDD(buffer).saveToEsWithMeta("spark/guofei_gd_py_corp_sharehd_info")
  }



  /**
    * 使用模板类描述表元数据信息
    * 鹏远企业股东信息
    */
  case class gd_py_corp_sharehd_info(id:String,batch_seq_num:String,
                                     name:String,contributiveFund:String,
                                     contributivePercent:String,currency:String,
                                     contributiveDate:String,corp_basic_info_id:String,
                                     query_time:String)

}



/*
    val col = Map("id"->d(0),
      "batch_seq_num"->d(1),
      "name"->d(2),
      "contributiveFund"->d(3),
      "contributivePercent"->d(4),
      "currency"->d(5),
      "contributiveDate"->d(6),
      "corp_basic_info_id"->d(7),
      "query_time"->d(8)
    )

    Map("gd_py_corp_sharehd_info" -> col)


 */