package cn.tarena

/**
 * 统计实体类
 */
class Tongji extends Serializable{
  private var uv = 0
  private var vv = 0
  private var pv =0
  private var newcust = 0
  private var newip = 0
  private var time=0L
  
  def setTime(newTime:Long)={
    this.time = newTime
  }
  def getTime()={
    this.time
  }
  
  def setUv(newuv:Int)={
    this.uv = newuv
  }
  def getUv()={
    this.uv
  }
  
  def setVv(newvv:Int){
    this.vv = newvv
  }
  def getVv()={
    this.vv
  }
  
  def setPv(newpv:Int)={
    this.pv = newpv
  }
  def getPv()={
    this.pv
  }
  def setNewCust(_newCust:Int)={
    this.newcust = _newCust
  }
  def getNewCust()={
    this.newcust
  }
  def setNewIp(_newIp:Int)={
    this.newip= _newIp
  }
  
  def getNewIp()={
    this.newip
  }
}