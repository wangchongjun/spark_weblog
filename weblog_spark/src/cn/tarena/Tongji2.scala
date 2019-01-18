package cn.tarena

/**
 * 统计平均深度   平均会话时间   跳出率
 */
class Tongji2 extends Serializable{
  
  private var br = 0d
  private var avgTime = 0d
  private var avgDeep = 0d
  
  
  def setBr(newBr:Double)={
    this.br = newBr
  }
  def getBr()={
    this.br
  }
  
  def setAvgTime(newAvgTime:Double)={
    this.avgTime = newAvgTime
  }
  def getAvgTime()={
    this.avgTime
  }
  
  def setAvgDeep(newAvgDeep:Double)={
    this.avgDeep = newAvgDeep
  }
  def getAvgDeep()={
    this.avgDeep
  }
}