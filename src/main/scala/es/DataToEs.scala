package es

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import org.elasticsearch.spark._
/**
  * Created by hc-3450 on 2016/12/2.
  */
object DataToEs {

  val data = new ListBuffer[Tuple2[String,immutable.Map[String,String]]]

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("dataToES").setMaster("local[2]")
    conf.set("es.nodes","rmhadoop01,rmhadoop02,rmhadoop03,rmhadoop04,rmhadoop05");
    conf.set("es.port","9200");
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.textFile("C:\\work\\ideabench\\SparkSQL\\data\\es\\gd_py_corp_sharehd_info.txt")
      .foreach(x=>{
        val element = x.split("""\t""")
        val eleMap = Map("id" ->element(0),
          "batch_seq_num" ->element(1),
          "name" ->element(2),
          "contributiveFund" ->element(3),
          "contributivePercent" ->element(4),
          "currency" ->element(5),
          "contributiveDate" ->element(6),
          "corp_basic_info_id" ->element(7),
          "query_time" ->element(8)
        )
        data.append((element(0),eleMap))
      })
    sc.makeRDD(data).saveToEsWithMeta("spark/test_gd_py_corp_sharehd_info")
    sc.stop()
  }

}
