/**
  * Created by Administrator on 2017/9/27.
  */
class SortKey(val clickCount:Long,val orderCount:Long,val payCount:Long) extends Ordered[SortKey] with  Serializable{
  override def compare(that: SortKey): Int = {
    if(clickCount != that.clickCount){
      (clickCount - that.clickCount).toInt
    }else if(orderCount != that.orderCount){
      (orderCount - that.orderCount).toInt
    }else if(payCount != that.payCount){
      (payCount - that.payCount).toInt
    }else{
      0
    }
  }
}
