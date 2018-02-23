/**
  * Created by Administrator on 2017/9/27.
  */
class SortKey(val clickCount:Long,val orderCount:Long,val payCount:Long) extends Ordered[SortKey] with  Serializable{
  override def compare(that: SortKey): Long = {
    if(clickCount != that.clickCount){
      clickCount - that.clickCount
    }else if(orderCount != that.orderCount){
      orderCount - that.orderCount
    }else if(payCount != that.payCount){
      payCount - that.payCount
    }else{
      0
    }
  }
}
