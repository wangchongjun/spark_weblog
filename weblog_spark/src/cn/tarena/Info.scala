package cn.tarena

/**
 * 清晰数据实体类
 */
class Info extends Serializable{
  private var url=""
  private var urlname=""
  private var uvid=""
  private var ssid =""
  private var sscount=""
  private var sstime=""
  private var cip =""
  
  def setUrl(newUrl:String)={
    this.url = newUrl
  }
  def getUrl()={
    this.url
  }
  def setUrlName(newUrlName:String)={
    this.urlname = newUrlName
  }
  def getUrlName()={
    this.urlname
  }
  def setUvId(newUvId:String)={
    this.uvid = newUvId
  }
  def getUvId()={
    this.uvid
  }
  def setSsId(newSsId:String)={
    this.ssid = newSsId
  }
  def getSsId()={
    this.ssid
  }
  def setSsCount(newSsCount:String)={
    this.sscount = newSsCount
  }
  def getSsCount()={
    this.sscount
  }
  def setSsTime(newSsTime:String)={
    this.sscount = newSsTime
  }
  def getSsTime()={
    this.sscount
  }
  def setCip(newCip:String)={
    this.cip = newCip
  }
  def getCip()={
    this.cip
  }
}