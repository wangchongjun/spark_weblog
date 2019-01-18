package cn.tarena

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import java.util.Calendar
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import java.util.Date
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
import java.sql.Timestamp

/**
 * 执行相应的业务逻辑   使用azkaban任务调度 定时执行任务
 */
object AvgDriver {
  
  
  val conf = new SparkConf().setMaster("local[3]").setAppName("Schedul")
  val sc = new SparkContext(conf)
  def main(args: Array[String]): Unit = {
   
    val tongji2 = new Tongji2()
    val deepList = new scala.collection.mutable.ListBuffer()
    //--计算获取5小时平均深度     会话深度/会话总数
    //--会话深度--同一会话的不同的urlname
    //首先获取过去5小时的所有记录  
    val regex = "^.*$"
    val date = new Date()
    val infoList = getDataByRegex(date.getTime+"", regex)
    
    //--平均深度  将list换成RDD
    val rdd = sc.makeRDD(infoList).map { x =>  (x.getSsId(),x.getUrlName())}.distinct()  
    val rdd2 = sc.makeRDD(infoList).map { x => x.getSsId() }.distinct()
    val count = rdd2.count().toDouble
    val avgDeep = rdd.count().toDouble/count
    
    //--平均会话时长
    val allTime = sc.makeRDD(infoList).map { x =>  
    (x.getSsId(),x.getSsTime().toLong)}
      .groupByKey.map { x => 
        print(x._2.max+","+x._2.min)
      val time = x._2.max - x._2.min 
      time
      }.reduce(_+_)
    val avgTime = allTime.toDouble/count
    
    //--跳出会话率  ssid数量为1 为跳出会话
    val allBr = sc.makeRDD(infoList).map { x =>(x.getSsId(),1)}.reduceByKey(_+_).filter(_._2==1)
    val avgBr = allBr.count().toDouble/count
    
    println("开始")
    //--将这三项指标进行入库操作
    tongji2.setBr(avgBr)
    tongji2.setAvgTime(avgTime)
    tongji2.setAvgDeep(avgDeep)
    myFun(date.getTime, tongji2)
    
  }
  
   /**
   * 将数据写入数据库
   */
  def myFun(time:Long,data :Tongji2): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement= null
    val sql = "insert into tongji2(stime,br,avgtime,avgdeep) values (?,?,?,?)"
    try {
        conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/weblog","root", "root")
     
        ps = conn.prepareStatement(sql)
        ps.setTimestamp(1, new Timestamp(time))
        ps.setDouble(2, data.getBr())
        ps.setDouble(3, data.getAvgTime())
        ps.setDouble(4, data.getAvgDeep())
        ps.executeUpdate()
    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
  
  
  
  
  def getDataByRegex(endTime:String,regex:String)={

    //--设置序列化方式
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02")
    hbaseConf.set("hbase.zookeeper.property.client", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "weblog")
    
    //--创建HBase扫描对象
    val scan = new Scan()
    val time = endTime
    val startTime = endTime.toLong-5*60*60*1000
    val filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex))
    //--获取当前时间
    scan.setStartRow((startTime+"").getBytes)
    scan.setStopRow(endTime.getBytes)
    //--设置过滤器
    scan.setFilter(filter)
    
    //--将scan对象设置生效
    hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()))
    
    //--通过HBase获取数据
    val resultRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
    //--生成保存数据的List
    val list  = scala.collection.mutable.ListBuffer[Info]()
    resultRDD.foreach{
      x=>
      val result = x._2
      val info = new Info()
      info.setUrl(Bytes.toString(result.getValue("cf1".getBytes, "url".getBytes)))  
      info.setUrlName(Bytes.toString(result.getValue("cf1".getBytes, "urlname".getBytes)))
      info.setUvId(Bytes.toString(result.getValue("cf1".getBytes, "uvId".getBytes)))
      info.setSsId(Bytes.toString(result.getValue("cf1".getBytes, "ssid".getBytes)))
      info.setSsCount(Bytes.toString(result.getValue("cf1".getBytes, "sscount".getBytes)))
      info.setSsTime(Bytes.toString(result.getValue("cf1".getBytes, "sstime".getBytes)))
      info.setCip(Bytes.toString(result.getValue("cf1".getBytes, "cip".getBytes)))
      list.append(info)  
    }
    //--返回相应的数据
    list
  }
}