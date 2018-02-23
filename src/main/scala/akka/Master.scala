package akka

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable

/**
  * Created by admin on 2017/7/29.
  */
class Master(val hostName:String,val port:Int) extends  Actor{//actor

      private val id2workerInfos = new mutable.HashMap[String,WorkerInfo]()
     //spark的源码里面还声明了另外一个集合去存储这个信息
        private val workerInfoes = new  mutable.HashSet[WorkerInfo]()


  override def preStart(): Unit = {
    import scala.concurrent.duration._
    import context.dispatcher
    context.system.scheduler.schedule(0 millis,15000 millis,self,CheckTimeOut)
  }

  /**
    * PartialFunction[Any, Unit]
    * 偏函数 是用来做模式匹配的
    * Any 传入的参数类型
    * Unit  返回值类型
    *
    * 这个方法启动以后一直做监听，你可以认为它就是一个死循环
    *
    * @return
    */
  override def receive: Receive ={
    case  RegisterWorker(workerID,cpu,memory) =>{

      //master接受到worker注册过来的信息
      /**
        * 1) map workerid,workerinfo(cpu.memory)  内存  hashMap
        *    <k,v>  workerID,WorkerInfo
        * 2）把信息保存到zookeeper里面
        */
      if(!id2workerInfos.contains(workerID)){
        val workerInfo = new WorkerInfo(workerID,cpu,memory)
        //保存worker信息到内存中
        id2workerInfos(workerID)=workerInfo
        workerInfoes += workerInfo

        //如果信息保存成功，那么 就是注册成功了！！！
        //master给worker发送了注册成功的信息
        sender() !  RegisteredWorker(s"masterURL:hostname:${hostName}, prot:${port}")
      }


    }

    case HeartBeat(workerID) =>{

      val currentTimeMillis = System.currentTimeMillis()

      val workerInfo = id2workerInfos(workerID)

      workerInfo.lastHeartBeatTime=currentTimeMillis

      id2workerInfos(workerID)=workerInfo
     workerInfoes+=workerInfo

    }

    case CheckTimeOut =>{

      //
    val currentTime=  System.currentTimeMillis()

      val deadWorker = workerInfoes.filter( w => currentTime - w.lastHeartBeatTime > 15000)
      //遍历超时的worker，从内存里面移除
      deadWorker.foreach(w =>{
        id2workerInfos -= w.workerId
        workerInfoes -= w
      })

      println("成功注册的worker个数"+workerInfoes.size);

    }
  }
}

object Master{

  val MASTER_ACTORM_NAME="MasterActorSystem"
  val MASTER_NAME="master"

  def main(args: Array[String]): Unit = {
    //这些一般都是从配置文件里面传进来的
    val hostName:String=args(0)
    val port:Int=args(1).toInt

    val str=
      s"""
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname ="${hostName}"
        |akka.remote.netty.tcp.port=${port}
      """.stripMargin
    val conf: Config = ConfigFactory.parseString(str)
     //创建actorSystem
    val actorSystem = ActorSystem(MASTER_ACTORM_NAME,conf)
    //创建并启动了这个actor
    actorSystem.actorOf(Props(new Master(hostName,port)),MASTER_NAME)

  }
}
