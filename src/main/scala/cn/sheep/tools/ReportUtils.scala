package cn.sheep.tools

import cn.sheep.beans.Logs

/**
  * Created by Administrator on 2017/8/15.
  * 总请求，有效请求，广告请求
  */
object ReportUtils {
  /**
    * 总请求，有效请求，广告请求
    * @param log
    * @return
    */
  def calculateAdRequest(log:Logs): List[Double] ={
    if(log.requestmode == 1){
       if(log.processnode == 1){
         List(1,0,0)
       }else if(log.processnode == 2){
         List(1,1,0)
       }else if(log.processnode == 3){
        List(1,1,1)
       }else{
        List(0,0,0)
       }
    }else{
     List(0,0,0)
    }
  }

  /**
    * 参与竞价数  竞价成功数的
    * @param log
    * @return
    */
  def calculateResponse(log:Logs):List[Double]={
    if(log.adplatformproviderid >= 100000 && log.iseffective == 1 && log.isbilling ==1){
       if(log.isbid == 1 && log.adorderid != 0){
         List(1,0)
       }else if(log.iswin == 1){
        List(0,1)
       }else{
         List(0,0)
       }
    }else{
      List(0,0)
    }
  }

  /**
    * 展示数 点击数
    * @param log
    * @return
    */
  def calculateShowClick(log:Logs):List[Double]={
    if(log.iseffective == 1){
       if(log.requestmode == 2){
       List(1,0)
       }else if(log.requestmode == 3){
         List(0,1)
       }else{
         List(0,0)
       }
    }else{
      List(0,0)
    }
  }

  /**
    * 广告消费  广告成本
    * @param log
    * @return
    */
  def calculateAdCost(log:Logs):List[Double]={
    if(log.adplatformproviderid >= 100000
      && log.iseffective == 1
      && log.isbilling ==1
      && log.iswin == 1
      && log.adorderid >= 200000
      && log.adcreativeid >=200000
    ){
     List(log.winprice/1000,log.adpayment/1000)
    }else{
      List(0.0,0.0)
    }
  }

}
