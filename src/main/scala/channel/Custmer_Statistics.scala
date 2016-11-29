package channel

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Michael on 2016/11/29.
  */
object Custmer_Statistics {
  case class blb_intpc_info(chnl_code:String,id_num:String)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custmer_Statistics").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    val blb_intpc_infoDF = sc.textFile("C:/work/ideabench/SparkSQL/data/channel/blb_intpc_info_10000_2.txt").map(_.split("\\t")).map(d => blb_intpc_info(d(0), d(1))).toDF()
    blb_intpc_infoDF.registerTempTable("blb_intpc_info")
    sqlContext.sql("select chnl_code,count(*) from blb_intpc_info group by chnl_code").collect().foreach(println)



























    /*    case class blb_intpc_info(intpc_id:Int,presona_id:Int,client_id:Int,
                                  loan_type:Int,loan_pird:Int,client_from:Int,other_src:String,intpc_state:Int,intpc_no:String,
                                  chnl_code:String,author_name:String,client_name:String,client_phone:String,id_num:String,comt_invest_dt:Int,
                                  final_contr_amt:Double,final_contr_rat:Double,final_contr_inst_type:Int,start_dt:String,
                                  end_dt:String,author:Int,staff_id:Int,data_dt:String,etl_dt:String,final_contr_mth_cost:Double,intpc_crt_dt:String)

        val DateInfoDF = sc.textFile("C:\\work\\ideabench\\SparkSQL\\data\\channel\\blb_intpc_info_10000.txt").
          map(
            _.split(",")).map(d => blb_intpc_info(d(0), d(1),d(2),d(3),d(4),d(5),d(6),d(7),d(8),d(9),
          d(10),d(11),d(12),d(13),d(14),d(15),d(16),d(17),d(18),d(19),d(20),d(21),d(22),d(23),d(24),d(25)
        )
        ).toDF()*/
    //定义case class用于后期创建DataFrame schema
    /*//对应Date.txt
    case class DateInfo(dateID:String,theyearmonth :String,theyear:String,themonth:String,thedate :String,theweek:String,theweeks:String,thequot :String,thetenday:String,thehalfmonth:String)
    //对应Stock.txt
    case class StockInfo(ordernumber:String,locationid :String,dateID:String)
    //对应StockDetail.txt
    case class StockDetailInfo(ordernumber:String,rownum :Int,itemid:String,qty:Int,price:Double,amount:Double)

    //加载数据并转换成DataFrame
    val DateInfoDF = sc.textFile("/data/Date.txt").map(_.split(",")).map(d => DateInfo(d(0), d(1),d(2),d(3),d(4),d(5),d(6),d(7),d(8),d(9))).toDF()
    //加载数据并转换成DataFrame
    val StockInfoDF= sc.textFile("/data/Stock.txt").map(_.split(",")).map(s => StockInfo(s(0), s(1),s(2))).toDF()
    //加载数据并转换成DataFrame
    val StockDetailInfoDF = sc.textFile("/data/StockDetail.txt").map(_.split(",")).map(s => StockDetailInfo(s(0), s(1).trim.toInt,s(2),s(3).trim.toInt,s(4).trim.toDouble,s(5).trim.toDouble)).toDF()

    //注册成表
    DateInfoDF.registerTempTable("tblDate")
    StockInfoDF.registerTempTable("tblStock")
    StockDetailInfoDF.registerTempTable("tblStockDetail")

    //执行SQL
    //所有订单中每年的销售单数、销售总额
    //三个表连接后以count(distinct a.ordernumber)计销售单数，sum(b.amount)计销售总额
    sqlContext.sql("select c.theyear,count(distinct a.ordernumber),sum(b.amount) from tblStock a join tblStockDetail b on a.ordernumber=b.ordernumber join tblDate c on a.dateid=c.dateid group by c.theyear order by c.theyear").collect().foreach(println)

*/
  }

}
