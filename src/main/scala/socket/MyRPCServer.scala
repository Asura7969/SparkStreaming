package socket

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.ServerSocket

/**
  * Created by Administrator on 2017/9/5.
  */
object MyRPCServer {
  /**
    * 处理注册信息
    * @param username
    * @param password
    */
  def handlerResiter(username:String,password:String): ResultMsg ={
    println("调用注册的方法")
    ResultMsg(1,"注册成功了")
  }
  def handlerHeartBeat(hostname:Int,state:String):ResultMsg ={
    println(s"$hostname  $state")
    ResultMsg(2,"心跳消息处理成功！！")
  }

  def main(args: Array[String]): Unit = {
    val serverSocket: ServerSocket = new ServerSocket(8888)
    val client = serverSocket.accept()
    //获取客户端传过来的流
    val stream: ObjectInputStream = new ObjectInputStream(client.getInputStream)

   //获取客户端的输出流对象
    val objectOutputStream = new ObjectOutputStream(client.getOutputStream)

   while (true){
     val clientMsg = stream.readObject()
     val result = clientMsg match {
       case RegisterMsg(username, password) => {
         handlerResiter(username, password)
       }

       case HeartBeat(hostname,state) =>{
         handlerHeartBeat(hostname,state)
       }

     }
     objectOutputStream.writeObject(result)
     objectOutputStream.flush()
   }





  }

}
