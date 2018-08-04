package graphX

import org.apache.spark.graphx.{Edge, EdgeContext, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object GraphXTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    val myVertices = sc.makeRDD(Array((1L,"Ann") , (2L,"Bill"), (3L,"Charles") , (4L,"Diane") , (5L,"Went to gym this morning")))

    val myEdges = sc .makeRDD(Array(Edge (1L, 2L,"is-friends-with"),
      Edge(2L , 3L,"is-friends-with"), Edge(3L, 4L,"is-friends-with"),
      Edge(4L , 5L,"Likes-status"), Edge(3L, 5L,"Wrote- status")))

    val myGraph = Graph(myVertices , myEdges)
    myGraph.vertices.collect.foreach(println)
    myGraph.edges.collect.foreach(println)
    println("triplets:有顺序的 点1 - 点2  边(关系)")
    myGraph.triplets.collect.foreach(println)

    println("1. 属性中包含 'is-friends-with' 的边： 2. 关系的源顶点属性中包含字母 a")
    myGraph.mapTriplets(t=> (t.attr,t.attr == "is-friends-with" && t.srcAttr.toLowerCase.contains("a")))
      .triplets.collect().foreach(println)


    println("统计每个顶点的出度")
    /*
        Msg : Int 函数的返回类型
        sendToSrc : 将 Msg 类型的消息发送给源顶点
        sendToDst : 将 Msg 类型的消息发送给目标顶点
        我们需要计算每个顶点发出的边数，所以在边上将包含整数 1 的消息发送到源顶点
        _ + _ : 匿名函数,累加操作
     */
    myGraph.aggregateMessages[Int](_.sendToSrc(1),_ + _)
      .rightOuterJoin(myGraph.vertices)
      .map(x => (x._2._2,x._2._1.getOrElse(0)))   //交换两个元素的顺序
      .collect().foreach(println)



  }

  /**
    * 此函数会在图中每条边上调用,只是简单的累加计数器
    * @param ec
    */
  def sendMsg(ec:EdgeContext[Int,String,Int]): Unit ={
    ec.sendToDst(ec.srcAttr + 1)
  }

  /**
    * 此函数会在所有的消息传递到顶点后被重复调用
    * 消息经过合并后,最终得出结果为包含最大距离值的顶点
    * @param a
    * @param b
    */
  def mergeMsg(a:Int,b:Int): Unit ={
    math.max(a,b)
  }


//  def propagteEdgeCount(g:Graph[Int,String]):Graph[Int,String]={
//    val verts = g.aggregateMessages[Int] (sendMsg, mergeMsg)
//    val g2 = Graph(verts,g.edges)
//    val check = g2.vertices.join(g.vertices).map(x=>x._2._1 - x._2._2).reduce(_ + _)
//    if(check > 0) propagteEdgeCount(g2) else g
//  }

}
