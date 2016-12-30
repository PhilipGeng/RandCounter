import generic.RandCounter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

object run {
  def main(args:Array[String]): Unit = {
    // val conf = new SparkConf().setAppName("shape rand counter with cache").set("spark.network.timeout","600").set("spark.akka.heartbeat.interval","100")
    // val sc = new SparkContext(conf)
    val sc = new SparkContext("local","randcount")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //val graph = GraphLoader.edgeListFile(sc, "hdfs:///fbdata/0.edges", false).partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d).convertToCanonicalEdges()
    /*
    val graph = GraphLoader.edgeListFile(sc, "/home/philip/IdeaProjects/RandomizedShapeCounter/data/complex.txt", false).partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d).convertToCanonicalEdges()
    val pattern: Array[Array[Int]] = Array(Array(1,2),Array(),Array())
    */
    val graph = GraphLoader.edgeListFile(sc, "/home/philip/IdeaProjects/RandomizedShapeCounter/data/tree.txt").partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d)
    //val graph = GraphLoader.edgeListFile(sc, "/home/philip/IdeaProjects/RandomizedShapeCounter/data/tri2.txt").partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d)
    //val graph = GraphLoader.edgeListFile(sc, "/home/philip/IdeaProjects/RandomizedShapeCounter/data/shape.txt").partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d)
    //val graph = GraphLoader.edgeListFile(sc, "/home/philip/dblp.in",true).partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d)
    //val pattern: Array[Array[Int]] = Array(Array(1),Array(2),Array(0))
    val pattern: Array[Array[Int]] = Array(Array(1,2),Array(3,4),Array(5),Array(),Array(),Array(),Array())
    //val pattern: Array[Array[Int]] = Array(Array(1,2),Array(),Array())

   // val res =  (1 to 10).map{i=>
    val res =  (10 to 10).map{i=>
      println("fraction+"+i/10d)
      val seq = (1 to 10).map{j=>
        val counter= new RandCounter(graph,false) // true: keep it ; false: to bidirection graph
        val r = counter.count(i/10d,pattern)
        //println(r)
        if(r.isNaN)
          0
        else
          r.toDouble
      }
      val mean = seq.sum/seq.length.toDouble
      val std = stddev(seq.toList,mean)
      val ret = (mean,std)
      println(ret)
      ret
    }
  }
  def stddev(xs: List[Double], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a,e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }
}