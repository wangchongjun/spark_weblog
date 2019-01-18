package cn.tarena

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import java.util.Calendar
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.RowFilter
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.RegexStringComparator
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Timestamp

/**
 * 程序入口类  与kafka进行整合   获取数据
 */
object Driver {
  
  val conf = new SparkConf().setMaster("local[10]").setAppName("resources").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "cn.tarena.kryo.MyKryoRegister")    
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val ssc=new StreamingContext(sc,Seconds(3))
    
    val zkHosts="hadoop01:2181,hadoop02:2181,hadoop03:2181"
  
  //--与kafka进行整合  获取数据
  
  //--定义相应的主题
  val topics = Map("weblog"->1)
  
  //--定义相应的消费者组
  val group = "gp"
 
  print("www")
  
  val data=KafkaUtils.createStream(ssc, zkHosts, group, topics).map{x=>x._2}.foreachRDD { x =>
  x.foreach { x => 
    wcj()
  val info = new Info()
  val datas = x.split("\\|")
  val ss = datas(14)
  datas.foreach(println)
  info.setUrl(datas(0))
  info.setUrlName(datas(1))
  info.setUvId(datas(13))
  info.setSsId(ss.split("_")(0))
  info.setSsCount(ss.split("_")(1))
  info.setSsTime(ss.split("_")(2))
  info.setCip(datas(15))
  //--设置需要保存的数据
  val tongji = new Tongji()
  tongji.setTime((info.getSsTime()).toLong)
  //--编辑pv
  tongji.setPv(1)
  
  //--编辑uv   通过时间戳 行键获取数据 "^\\d{13}_" + uvid + "_\\d{10}_\\d{1,2}$"
  val regex = "^\\d{13}_"+info.getUvId()+"_\\d{10}_\\d{1,2}$"

  val arr = getDataByRegex(info.getSsTime(),regex)

  if(arr.length == 0){
    tongji.setUv(1)
  }else{
    tongji.setUv(0)
  }
  
  //--编辑vv 通过时间戳行键获取数据  
  val regex1 = "^\\d{13}_\\d{20}_"+info.getSsId()+"_\\d{1,2}$"
  val ssArr = getDataByRegex(info.getSsTime(), regex1)
  if(ssArr.length == 0){
    tongji.setVv(1)
  }else{
    tongji.setVv(0)
  }
  
  //--编辑newcust  新增用户数
  val custList  = getDataByCol("cf1","uvId",info.getUvId())
  if(custList.size == 0){
    tongji.setNewCust(1)
  }else{
    tongji.setNewCust(0)
  }
  
  //--编辑newip  新增IP数
  val ipList = getDataByCol("cf1", "cip", info.getCip())
  if(ipList.size ==0){
    tongji.setNewIp(1)
  }else{
    tongji.setNewIp(0)
  }
  print("dididi")
  //--将数据保存到数据库
  myFun(tongji)
  
  //--将数据写入hbase
  saveData(info)
  print("over")
      }
    }
  //--保持线程一直执行
  ssc.start()
  ssc.awaitTermination()
  }
  
  def wcj()={
    println("wcj")
  }
  /**
   * 将数据写入数据库
   */
  def myFun(data :Tongji): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement= null
    val sql = "insert into tongji(date,pv,uv,vv,newip,newcust) values (?,?,?,?,?,?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/weblog","root", "root")
     
        ps = conn.prepareStatement(sql)
        ps.setTimestamp(1, new Timestamp(data.getTime()))
        ps.setInt(2, data.getPv())
        ps.setInt(3, data.getUv())
        ps.setInt(4, data.getVv())
        ps.setInt(5,data.getNewIp())
        ps.setInt(6, data.getNewCust())
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
  
  /**
   * 从Hbase获取数据 根据时间  和正则进行匹配
   */
  def getDataByRegex(endTime:String,regex:String)={
    println("wcj")
    //--设置序列化方式
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02")
    hbaseConf.set("hbase.zookeeper.property.client", "2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "weblog")
    
    //--创建HBase扫描对象
    val scan = new Scan()
    val time = endTime
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time.toLong)
    cal.set(Calendar.HOUR,0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MILLISECOND, 0)
    val filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex))
    //--获取当前时间
    scan.setStartRow(String.valueOf(cal.getTimeInMillis).getBytes)
    scan.setStopRow(endTime.getBytes)
    //--设置过滤器
    scan.setFilter(filter)
    
    //--将scan对象设置生效
    hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()))
    
    //--通过HBase获取数据
    val resultRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
    //--生成保存数据的List
    val rdd = resultRDD.map{
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
      info  
    }.collect()
    print("长度为:"+rdd.size)
    //--返回相应的数据
    rdd
  }
  
  /**
   * 根据列族 列名 列值 获取数据
   */
  def getDataByCol(cfName:String,colName:String,colValue:String)={
    
    //--设置序列化方式
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02")
    hbaseConf.set("hbase.zookeeper.property.client","2181" )
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "weblog")
    
    //--创建HBase扫描对象
    val scan = new Scan()
    val filter = new SingleColumnValueFilter(cfName.getBytes,colName.getBytes,CompareOp.EQUAL,colValue.getBytes)
    scan.setFilter(filter)
    
    //--将scan对象设置生效
    hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()))
    
    //--获取结果RDD
    val resultRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    
    //--进行结果的遍历
   val rdd = resultRDD.map{
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
      info
    }.collect()
    //--返回结果值
    rdd
  }
  
  /**
   * 向Hbase写入数据
   */
   def saveData(data:Info)={
     //--整合hbase
     sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "hadoop01,hadoop02")
     sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
     sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "weblog")
     
     //--声明job
     val job = new Job(sc.hadoopConfiguration)
     job.setOutputKeyClass(classOf[ImmutableBytesWritable])
     job.setOutputValueClass(classOf[Result])
     job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
     
     //--创建行键   会话时间 _用户ID_会话ID
     val rowKey = data.getSsTime()+"_"+data.getUvId()+"_"+data.getSsId()+"_"+(Math.random()*100).toInt
     
     val put = new Put(rowKey.getBytes)
     
     //--将数据填入put
     put.add("cf1".getBytes(), "url".getBytes(), data.getUrl().getBytes());
     put.add("cf1".getBytes(), "urlname".getBytes(), data.getUrlName().getBytes());
     put.add("cf1".getBytes(), "uvId".getBytes(), data.getUvId().getBytes());
     put.add("cf1".getBytes(),"ssid".getBytes(),data.getSsId().getBytes());
     put.add("cf1".getBytes(), "sscount".getBytes(), data.getSsCount().getBytes());
     put.add("cf1".getBytes(), "sstime".getBytes(), data.getSsTime().getBytes());
     put.add("cf1".getBytes(), "cip".getBytes(), data.getCip().getBytes());
     
     val infos  = (new ImmutableBytesWritable,put)
     val hbaseRDD = sc.makeRDD(Array((new ImmutableBytesWritable,put)))
     
     //--执行插入操作
     hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
   }
}