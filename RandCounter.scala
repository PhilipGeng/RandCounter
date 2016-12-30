package generic
import org.apache.spark.graphx._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by philip on 9/10/16.
  */

class RandCounter[VD:ClassTag, ED:ClassTag](val g:Graph[VD,ED], val direction: Boolean) extends Serializable {
  //convert to bidirection graph (or not)
  val graph:Graph[VD,ED] = if(direction) g else toBidirectionGraph(g)
  //count num of vertices
  val totalVertices:Int = graph.numVertices.toInt

  //convert to bidirection graph
  private def toBidirectionGraph[VD:ClassTag, ED:ClassTag](g:Graph[VD,ED]): Graph[VD,ED]={
    val bdEdges = g.edges.union(g.reverse.edges)
    val bdGraph = Graph(g.vertices, bdEdges).groupEdges((a,b)=>a)
    bdGraph
  }

  //init every vertex by XVert, with neighborhood information and length of pattern (max # of distinct vertices)
  private def initGraph(graph: Graph[VD,ED], n:Int): Graph[XVert,ED] ={
    val nbrGraph = graph.collectNeighborIds(EdgeDirection.Out)
    graph.outerJoinVertices(nbrGraph) { (vid, g, nbr) => new XVert(vid.toInt, nbr.orNull.map(x => x.toInt),n)}
  }

  //random select vertices as start point and append msg
  private def initMsg(initG: Graph[XVert,ED], fraction:Double, n:Int): Graph[XVert,ED] ={
    initG.mapVertices { (vid, xv) => //encapsulate msg format
      if(scala.util.Random.nextDouble() < fraction){
        val pathArr = Array.fill(n){-1}
        pathArr(0) = vid.toInt
        xv.addMsg(pathArr, 0, totalVertices)
      }
      xv
    }
  }

  def count(fraction: Double, pattern: Array[Array[Int]]): Double ={
    var currentIteration = 0
    val n = pattern.length
    val initG = initGraph(graph, n)
    var runtimeGraph = initMsg(initG, fraction,n).cache()

    val totalCount = runtimeGraph.vertices.filter(x=>x._2.getMsg(0).length!=0).count()
//    val bp1 = runtimeGraph.vertices.collect()

    while (currentIteration < pattern.length) {
      //println("iteration: "+currentIteration)
      val patNbr: Array[Int] = pattern(currentIteration)
      if(patNbr.length!=0){
        //generate next step in current msglist
        runtimeGraph = runtimeGraph.mapVertices{(vid,xv)=> xv.genNextStep(patNbr,currentIteration)}
//        val bp2 = runtimeGraph.vertices.collect()
        if(currentIteration < pattern.length-1){
          //regenerate a graph, a bug of inconsistency on triplet vertex view
          runtimeGraph = Graph(runtimeGraph.vertices,runtimeGraph.edges)

          //aggregate message
          val communicateGraph = runtimeGraph.aggregateMessages[XVert](
            triplet => {
              var filterMsg: ArrayBuffer[Msg] = triplet.srcAttr.getMsg(currentIteration)
              //filter message that contains triplet dstid
              filterMsg = filterMsg.filter(msg=>msg.nextStep.toSet.contains(triplet.dstId.toInt))
              //set nextstep in path in passMsg
              val passMsg = filterMsg.map{m=>
                m.setPath(patNbr)
                m
              }
              passMsg.foreach{x=>
                x.nextStep.zip(patNbr).zip(x.nextProb).foreach{ case ((step,p),i)=>
                  if(triplet.dstId.toInt == step){
                    //generate a new XVert
                    val ms = new Msg(x.path, if(i == -1) 1 else triplet.srcAttr.nbr.length-i ,p)
                    val passV = triplet.dstAttr.dupVertNewMsg(ms,ms.iter)
                    triplet.sendToDst(passV)
                  }
                }
              }
            },
            (v1, v2) => {
              v1.mergeMsg(v2)
              v1
            }
          )
//          val bp3 = communicateGraph.collect()
          runtimeGraph = runtimeGraph.outerJoinVertices(communicateGraph) { (vid, oldV, newV) => oldV.mergeMsg(newV.orNull) }
        }
      //synchronization barrier for some local variables
       runtimeGraph.vertices.cache().foreach{x=>x}
      //  val bp4 = runtimeGraph.vertices.collect()
      //  val a = 1
      }
      currentIteration += 1
    }
    //   runtimeGraph.vertices.flatMap(x=>x._2.msgList.flatten).groupBy(_.start).map{case(id,msgList)=>
    //    msgList.reduce()
    // }
    val resvert = runtimeGraph.vertices.collect()
    val resGraph = runtimeGraph.vertices.map(x=>x._2.msgList.flatten).flatMap(x=>x).map(x=>(x.start,x.probability))
    val a = resGraph.collect()
    val res = resGraph.reduceByKey(_*_).cache()
    val cres = res.collect()
    val r = res.map(x=>x._2).sum()/totalCount
    r
  }
}
