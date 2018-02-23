package akka

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by admin on 2017/7/29.
  */
class Worker(val masterHost:String,val masterPort:Int,val cpu:Int,val memory:Int) extends Actor{
  var masterRef: ActorSelection=_
  val workerID = UUID.randomUUID().toString
  /**
    * 这是生命周期的方法
    */
  override def preStart(): Unit = {


    masterRef= context.actorSelection(s"akka.tcp://${Master.MASTER_ACTORM_NAME}@${masterHost}:${masterPort}/user/${Master.MASTER_NAME}")
    masterRef ! RegisterWorker(workerID,cpu,memory)
  }

  override def receive: Receive = {
    case RegisteredWorker(masterURL) =>{
      println("我是woker，我想master注册成功以后，master给我发送的URL"+masterURL);

      /**
        * 1) 延迟多长时间以后开始执行
        * 2) 每隔多长时间执行一次
        * 3)给谁发
        * 4）发什么
        * 因为是实际的架构模式里面，我们就不能这样直接去发送心跳
        *   一般在发送心跳之前去做好多准备工作
        */
      import scala.concurrent.duration._
      import context.dispatcher
      context.system.scheduler.schedule(0 millis,10000 millis,self,SendHeartBeat )

    }

    case SendHeartBeat=>{
      //做好多准备工作


      //向master发送心跳
      masterRef ! HeartBeat(workerID)
    }
  }
}

object Worker{

  val WORKER_ACTOR_NAME="WorkerActorSystem"
  val WORKER_NAME="worker"

  def main(args: Array[String]): Unit = {

    val host=args(0)
    val masterHost=args(1)
    val masterPort=args(2).toInt
    val cpu=args(3).toInt
    val memory=args(4).toInt
    val port=args(5).toInt
    val str=
      s"""
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = "${host}"
        |akka.remote.netty.tcp.port = "${port}"
      """.stripMargin
    val conf: Config = ConfigFactory.parseString(str)
    val actorSystem: ActorSystem = ActorSystem(WORKER_ACTOR_NAME,conf)
    //创建并启动了worker 的actor

    /**
      * 1:主构造函数
      * 2：
      * 3：receive
      */
    actorSystem.actorOf(Props(new Worker(masterHost,masterPort,cpu,memory)),WORKER_NAME)
  }

}
