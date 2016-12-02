package es


import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import utils.PropertiesUtils

/**
  * 统计不同渠道进件数量
  * Created by Michael on 2016/11/29.
  */
object Spark_ES {

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
    //读取本地文件
    val result = sc.textFile("C:/work/ideabench/SparkSQL/data/es/gd_py_corp_sharehd_info.txt")
      .map(_.split("\\t"))
      .map(d =>{
          if(PropertiesUtils.getStringByKey("gd_py_corp_sharehd_info").equals("one2many")){
            Map(d(0) ->gd_py_corp_sharehd_info(d(0), d(1), d(2), d(3), d(4), d(5), d(6), d(7), d(8)))
          }else if(PropertiesUtils.getStringByKey("gd_py_corp_sharehd_info").equals("one2one")){
            Map(d(1) ->gd_py_corp_sharehd_info(d(0), d(1), d(2), d(3), d(4), d(5), d(6), d(7), d(8)))
          }

      } )

    result.saveToEs("spark/map_gd_py_corp_sharehd_info")
  }

}

