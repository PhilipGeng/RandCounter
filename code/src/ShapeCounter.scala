import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.SparkContext

/**
  * Created by Philip on 2016/8/31.
  */
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
// $example off$
object ShapeCounter {
  def main(args:Array[String]): Unit ={
    val sc = new SparkContext("local","shapeCounter")
    val graph = GraphLoader.edgeListFile(sc, "src/shape.txt", true)
      .partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    val Count = triCounts.map(x=>x._2).sum/3
    println(Count)

    // Join the triangle counts with the usernames
    /* val users = sc.textFile("src/users.txt").map { line =>
       val fields = line.split(",")
       (fields(0).toLong, fields(1))
     }
     val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
       (username, tc)
     }
     // Print the result
     println(triCountByUsername.collect().mkString("\n"))
     */
  }
}
