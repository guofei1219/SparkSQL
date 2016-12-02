package channel

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计不同渠道进件数量
  * Created by Michael on 2016/11/29.
  */
object Custmer_Statistics {

  /**
    * 使用模板类描述表元数据信息
    * @param chnl_code
    * @param id_num
    */
  case class blb_intpc_info(chnl_code:String,id_num:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custmer_Statistics").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //RDD隐式转换成DataFrame
    import sqlContext.implicits._
    //读取本地文件
    val blb_intpc_infoDF = sc.textFile("C:/work/ideabench/SparkSQL/data/channel/blb_intpc_info_10000_2.txt")
      .map(_.split("\\t"))
      .map(d => blb_intpc_info(d(0), d(1))).toDF()

    //注册表
    blb_intpc_infoDF.registerTempTable("blb_intpc_info")

    /**
      * 分渠道进件数量统计并按进件数量降序排列
      */
    blb_intpc_infoDF.registerTempTable("blb_intpc_info")
    sqlContext.sql("" +
      "select chnl_code,count(*) as intpc_sum " +
      "from blb_intpc_info " +
      "group by chnl_code").toDF().sort($"intpc_sum".desc).show()
  }

}

/*
    case class blb_intpc_info(intpc_id:Int,presona_id:Int,client_id:Int,
                              loan_type:Int,loan_pird:Int,client_from:Int,other_src:String,intpc_state:Int,intpc_no:String,
                              chnl_code:String,author_name:String,client_name:String,client_phone:String,id_num:String,comt_invest_dt:Int,
                              final_contr_amt:Double,final_contr_rat:Double,final_contr_inst_type:Int,start_dt:String,
                              end_dt:String,author:Int,staff_id:Int,data_dt:String,etl_dt:String,final_contr_mth_cost:Double,intpc_crt_dt:String)
    val blb_intpc_infoDF = sc.textFile("C:\\work\\ideabench\\SparkSQL\\data\\channel\\blb_intpc_info_10000.txt").
    map(_.split("\\t")).
    map(d =>
    blb_intpc_info(d(0).toInt, d(1).toInt,d(2).toInt,d(3).toInt,d(4).toInt,d(5).toInt,d(6),d(7).toInt,d(8),d(9),
    d(10),d(11),d(12),d(13),d(14).toInt,d(15).toDouble,d(16).toDouble,d(17).toInt,d(18),
    d(19),d(20).toInt,d(21).toInt,d(22),d(23),d(24).toDouble,d(25)
    )
    ).toDF()
*/