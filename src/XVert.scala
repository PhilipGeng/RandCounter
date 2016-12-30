package generic

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by philip on 9/15/16.
  */

class XVert(val id: Int, val nb: Array[Int], val n:Int) extends Serializable {
  val vid = id
  val nbr: Array[Int] = nb
  val pat_len = n
  var msgList: Array[ArrayBuffer[Msg]] = new Array[ArrayBuffer[Msg]](n).map(x=>new ArrayBuffer[Msg]())
  def addMsg(path:Array[Int], iter:Int, prob:Double): Unit ={
    msgList(iter).append(genMsg(path, prob, iter))
  }
  def genMsg(path:Array[Int], prob:Double, iter: Int): Msg = new Msg(path, prob, iter)
  def getMsg(iter:Int): ArrayBuffer[Msg] =msgList(iter)
  def resetMsg(msg:Msg, i:Int): Unit ={
    val m = new Array[ArrayBuffer[Msg]](n).map(x=>new ArrayBuffer[Msg]())
    m(i).append(msg)
  }

  // randomly assign all msg.nextStep in msglist
  def genNextStep(pat:Array[Int], iter:Int): XVert ={
    val l = pat.length
    msgList(iter).foreach{x=>
      //print("start: "+x.start)
      //rand methodology
      var rArr = Random.shuffle(nbr.toSeq)
      if(x.probability!=0){ //probability==0: already invalid path, dont process it
        if(rArr.length >= l){ // have sufficient number of neighbors
          var determinedVertInNbr = true
          val rdNext = pat.map{p=>
            val inPath = x.path(p)
            if(inPath != -1){
              if(nbr.contains(inPath))
                inPath
              else{
                determinedVertInNbr = false
                -1
              }
            }
            else{
              val selected = rArr.take(1).head
              rArr = rArr.drop(1)
              selected
            }
          }
          var innerCounter = -1
          val nextProb = pat.map{p=>
            val inPath = x.path(p)
            if(inPath != -1)
              -1
            else{
              innerCounter = innerCounter+1
              innerCounter
            }
          }
          //print(" rdNext: "+rdNext.toSeq+" nextProb: "+nextProb.toSeq)

          if(validateMsg(rdNext,pat,x.path) && determinedVertInNbr){
            //print("accepted")
            x.setNext(rdNext,nextProb)
          }
          else{
            //print("ignored")
            x.probability = 0
          }
        }
        else{
          //print(" ignored")
          x.probability = 0
        }
      }
      //println("")
    }
    this
  }

  private def validateMsg(rdNext: Array[Int], pat: Array[Int], path: Array[Int]): Boolean ={
    //print("; path: "+path.toSeq +"; ")
    val validate = rdNext.zip(pat).map{x=>
      val randNbr = x._1
      val patNbr = x._2
      if(path(patNbr) != -1 && path(patNbr) != randNbr) 1 else 0
    }.sum
    if(validate == 0 && rdNext.length==pat.length) true else false
  }

  def dupVertNewMsg(msg:Msg, i:Int): XVert = {
    /*val v = this.clone()
    v.resetMsg(msg,i)
    v*/
    val m = new Array[ArrayBuffer[Msg]](n).map(x=>new ArrayBuffer[Msg]())
    m(i).append(msg)
    val nXVert = new XVert(vid, nbr, n)
    nXVert.msgList = m
    nXVert
  }

  def mergeMsg(v2: XVert): XVert ={
    if(v2!=null){
      val ml: Array[ArrayBuffer[Msg]] = msgList.zip(v2.msgList).map{case(v1m,v2m)=>v1m++v2m}
      msgList = ml
    }
    this
  }

  def showDetail(): Unit = {
    print("vid: "+vid+" nbr:")
    nbr.foreach(x=>print(x+","))
    msgList.foreach { x =>
      x.foreach{a =>
        print("(start: "+a.start+"; path: ")
        a.path.foreach(p=>print(p+","))
        print("; iteration: "+a.iter+"; probability:"+a.probability+"; nextStep:")
        if(a.nextStep.length==0) print("null")
          a.nextStep.foreach(ns=>print(ns))
        print(")")
      }
      println()
    }
  }
}
