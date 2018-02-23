package socket

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.Socket

/**
  * Created by Administrator on 2017/9/5.
  */
object MyRPCClient {

  def main(args: Array[String]): Unit = {
    val socket = new Socket("localhost",8888)

    val oos = new ObjectOutputStream(socket.getOutputStream)
    //获取到输入流
    val ois = new ObjectInputStream(socket.getInputStream)


    oos.writeObject(RegisterMsg("liuteng","7btigs"))
    oos.flush()

    val msg = ois.readObject()
   // println(msg)

    oos.writeObject(HeartBeat(123,"alive"))
    oos.flush()

    val heartbeatMsg=ois.readObject()
    println(heartbeatMsg)

    oos.close()
    ois.close()
    socket.close()




  }

}
