package org.apache.spark.graphx.lib

import org.apache.spark
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import org.apache.log4j.Logger
import org.apache.log4j.Level

object RandomizedShapeCounter {
  val fraction = 1
  val n = 3
  def main(args:Array[String]): Unit = {
    val sc = new SparkContext("local", "shapeCounter")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val graph = GraphLoader.edgeListFile(sc, "src/0.edges", false).partitionBy(PartitionStrategy.RandomVertexCut).mapEdges(e => 1d).convertToCanonicalEdges()
    val bdEdges = graph.edges.union(graph.reverse.edges)
    val bdGraph = Graph(graph.vertices, bdEdges)
    //bdGraph.edges.collect().foreach(println)
    val totalVertices = graph.numVertices
    var currentIteration = 0

    val initGraph = bdGraph.outerJoinVertices(graph.degrees) { (vid, graphvalue, degree) => //calculate degree info
      degree match {
        case Some(degree) => degree
        case None => 0 // No outDegree means zero outDegree
      }
    }.mapTriplets { triplet => //edge attr = src degree
      triplet.srcAttr.toDouble
    }.mapVertices { (vid, value) => //encapsulate msg format
      //startid,path_vertices,probability,#_of_iteration,nextStep
      var msgarr: Array[(VertexId, VertexSet, Double, Int, Int)] = Array()
      if (scala.util.Random.nextDouble() < fraction) {
        val pathSet = new VertexSet(n)
        pathSet.add(vid)
        msgarr = Array((vid, pathSet, (1d / totalVertices), currentIteration,-1))
      }
      msgarr
    }

    val totalCounted = initGraph.vertices.filter(x=>x._2.length!=0).count()
    val degreeGraph = initGraph.collectNeighborIds(EdgeDirection.Either)

    var runtimeGraph = initGraph
    while (currentIteration < n-1) {
      runtimeGraph = randNextStep(runtimeGraph,degreeGraph)
      runtimeGraph.vertices.collect.foreach { x =>
        print(x._1 + ":")
        x._2.foreach { x =>
          print("("+x._1 + ",")
          print(x._2)
          print("," + x._3 + "," + x._4 + "," + x._5)
          println(") ")
        }
      }
      println("///")

      currentIteration += 1
      val iterated = runtimeGraph.aggregateMessages[Array[(VertexId, VertexSet, Double, Int, Int)]](
        triplet => {
          val sampledMsg = triplet.srcAttr.filter(x=>x._5==triplet.dstId)
          val filteredMsg = sampledMsg.filter(x =>
            !x._2.contains(triplet.dstId)
          )
          val passMsg = filteredMsg.map { m =>
            val (startId, pathSet, p, iteration, nextStep) = m
            pathSet.add(triplet.dstId)
            var newProb=0d
            if(currentIteration>n-1)
              newProb = p
            else
              newProb = p / triplet.attr
            require(iteration == currentIteration - 1)
            (startId, pathSet, newProb, currentIteration, nextStep)
          }
          triplet.sendToDst(passMsg)
        },
        (a, b) => a ++ b
      )
      runtimeGraph = runtimeGraph.outerJoinVertices(iterated) { (vid, oldmsg, newmsg) => newmsg.getOrElse(null) }
    }
    runtimeGraph.vertices.collect.foreach { x =>
      print(x._1 + ":")
      x._2.foreach { x =>
        print("("+x._1 + ",")
        print(x._2)
        print("," + x._3 + "," + x._4 + "," + x._5)
        println(") ")
      }
    }
    println("//")

    val resGraph = runtimeGraph.outerJoinVertices(degreeGraph) { (vid, msg, nb) =>
      val nbr = nb.getOrElse(null)
      val values = msg.filter(x => nbr.contains(x._1)).map(x=>1/x._3)
      values.sum
    }

    val s = resGraph.vertices.collect().map(x=>x._2).sum
    println(totalCounted)

    println(s/totalCounted/6)
      //    val res = resGraph.vertices.collect.map{x=> x._2.map(x=>1/x._3).sum}.sum/6
   // println("total count: "+res)
    sc.stop()
  }

  def randNextStep[VD:ClassTag, ED:ClassTag](graph: Graph[Array[(VertexId, VertexSet, Double, Int, Int)],Double], degreeGraph:RDD[(VertexId,Array[VertexId])]): Graph[Array[(VertexId, VertexSet, Double, Int, Int)],Double] ={
    graph.outerJoinVertices(degreeGraph){(vid,msg,nbr)=>
      val nbarr = nbr.getOrElse(null)
      val randMsg = msg.map(x=>(x._1,x._2,x._3,x._4, nbarr(scala.util.Random.nextInt(nbarr.length-1)))).map(x=>(x._1,x._2,x._3,x._4,x._5.toInt))
      randMsg
    }
  }
}