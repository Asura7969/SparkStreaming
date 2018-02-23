package akka

/**
  * Created by admin on 2017/7/29.
  */
//worker -> master
case class RegisterWorker(val workerID:String,val cpu:Int,val memory:Int) extends Serializable
//master -> worker
case class RegisteredWorker(val masterURL:String) extends  Serializable
//worker -> master
case class HeartBeat(val workerID:String) extends  Serializable

case object SendHeartBeat
case object CheckTimeOut