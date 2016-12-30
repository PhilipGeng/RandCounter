package generic

/**
  * Created by philip on 9/15/16.
  */
class Msg(val p: Array[Int], val prob: Double, val i: Int) extends Serializable{
  val path = p
  var nextStep: Array[Int] = Array[Int]()
  var probability = prob
  val start = p(0)
  val iter: Int = i
  var nextProb:Array[Int] = Array[Int]()
  def setNext(n:Array[Int],np:Array[Int]): Unit ={
    nextStep = n
    nextProb = np
  }
  def setPath(pat: Array[Int]): Unit ={
    nextStep.zip(pat).foreach{case (step,pt)=>
      path(pt) = step
    }
  }

}
