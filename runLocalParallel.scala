package generic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object runLocalParallel {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("randCount").set("spark.network.timeout","600").set("spark.akka.heartbeat.interval","100").setMaster("local[4]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val graph = GraphLoader.edgeListFile(sc, "/home/philip/dblp.in",true).partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d)
    val pattern: Array[Array[Int]] = Array(Array(1),Array(2),Array(0))
    var t1 = System.currentTimeMillis()
    var count = graph.triangleCount().vertices.reduce((a,b)=>(a._1,a._2+b._2))._2
    var t2 = System.currentTimeMillis()
    println(t2-t1)
  }

}