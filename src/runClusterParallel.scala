package generic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object runClusterParallel {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("randCount").set("spark.network.timeout","600").set("spark.akka.heartbeat.interval","100")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val graph = GraphLoader.edgeListFile(sc, "hdfs:///graph/dblp.in",true).partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d)
    val pattern: Array[Array[Int]] = Array(Array(1),Array(2),Array(0))

    val res =  (1 to 10).map{i=>
      println("fraction+"+i/10d)
      val counter= new RandCounter(graph,false) // true: keep it ; false: to bidirection graph
      var t1 = System.currentTimeMillis()
      val r = counter.count(i/10d,pattern)
      var t2 = System.currentTimeMillis()
      (t2-t1)
    }
    println(res.toSeq)
  }

}
