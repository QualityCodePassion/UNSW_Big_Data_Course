package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Ass3 {

  val Id = 0
  val PostId = 1
  val VoteTypeId = 2
  val UserId = 3
  val CreationTime = 4

  /** A function that splits a line of input into (voteTypeId, postId) tuples. */
  def parseLine(line: String) = {
    val fields = line.split(",")
    val voteTypeId = fields(VoteTypeId).toInt
    val postId = fields(PostId)
    // Create a tuple that is our result.
    (voteTypeId, postId)
  }
  
    /** Code for Q1 
     *  input - VoteTypeId (key) and PostId
     *  returns - the VoteTypeId that is associated with the fewest posts and the list of posts. 
     *  */
  def Question1(voteTuple: RDD[(Int,String)]) : (String, Iterable[String]) = {
    
    // Count how many post are ascociated with each VoteTypeId and construct a list of all the post
    val totalsByVoteTypeId = voteTuple.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + "," + y._1 , x._2 + y._2))
    
    // Based on http://stackoverflow.com/questions/26886275/how-to-find-max-value-in-pair-rdd
    // Obtain the VoteTypeId with the min count
    val minValue = totalsByVoteTypeId.min()(new Ordering[Tuple2[Int, (String, Int)]]() {
      override def compare(x: (Int, (String, Int) ), y: (Int, (String, Int) )): Int = 
      Ordering[(Int)].compare(x._2._2, y._2._2)
      })
    
    // Obtain, then Sort and print the final results.
    //val results = totalsByVoteTypeId.collect()
    //results.sorted.foreach(println)
    
    val minVoteTypeId = minValue._1.toString()
    var listOfPostId : Iterable[String] = minValue._2._1.split(",")
    
    (minVoteTypeId, listOfPostId)
  }

  
  /*
  def Question2(textFile: RDD[Array[String]]): List[Int] = {
	  //write your code
   

  }*/


  def main(args: Array[String]) {
    val inputFile = args(0)    
    val conf = new SparkConf().setAppName("Ass3").setMaster("local")
    val sc = new SparkContext(conf)

    //val textFile = sc.textFile(inputFile).map(_.split(","))
    val textFile = sc.textFile(inputFile)
    // Use our parseLines function to convert to (voteTypeId, postId) tuples
    val voteTuple = textFile.map(parseLine)

    
    val votetype = Question1(voteTuple)
    println(votetype)
    //val posts = Question2(textFile)
    //println(posts)
  }
}
