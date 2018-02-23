package socket

/**
  * Created by Administrator on 2017/9/5.
  */
trait RemoteMessage extends  Serializable
//注册消息
case class RegisterMsg(username:String,password:String) extends  RemoteMessage
//结果消息
case class ResultMsg(id:Int,context:String) extends  RemoteMessage
//心跳消息
case class HeartBeat(id:Int,context:String) extends RemoteMessage
