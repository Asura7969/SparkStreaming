package akka

/**
  * Created by admin on 2017/7/29.
  *
  */
/**
  * 这个用来保存worker的信息
  * @param workerId  每个worker独一无二的id号
  * @param cpu
  * @param memory
  */
class WorkerInfo(val workerId:String,val cpu:Int,val memory:Int) {
  var lastHeartBeatTime:Long =_

}
