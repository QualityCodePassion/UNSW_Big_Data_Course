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

  
  /** Code for Q1 
  *  input - RDD Array of strings of the input text
  *  returns - the VoteTypeId that is associated with the fewest posts and the list of posts. 
  */
  def Question1(textFile: RDD[Array[String]]) : (String, Iterable[String]) = {
    
    // convert the array of strings into an RDD tuple of VoteTypeId (key) and PostId
    val convertToTuple = textFile.map( x => (x(VoteTypeId).toInt, x(PostId) ) )
    // Count how many post are ascociated with each VoteTypeId and construct a list of all the post
    val totalsByVoteTypeId = convertToTuple.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + "," + y._1 , x._2 + y._2))
    
    // Based on http://stackoverflow.com/questions/26886275/how-to-find-max-value-in-pair-rdd
    // Obtain the VoteTypeId with the min count
    val minValue = totalsByVoteTypeId.min()(new Ordering[Tuple2[Int, (String, Int)]]() {
      override def compare(x: (Int, (String, Int) ), y: (Int, (String, Int) )): Int = 
      Ordering[(Int)].compare(x._2._2, y._2._2)
      })
    
    val minVoteTypeId = minValue._1.toString()
    var listOfPostId : Iterable[String] = minValue._2._1.split(",")
    
    (minVoteTypeId, listOfPostId)
  }

  /** Code for Q2
  *  input - RDD Array of strings of the input text
  *  returns - Find the Ids of all posts that are favoured by more than
  *  five users. Your output should only contain PostIds, sorted in descending
  *  order according to their NUMERIC values.
  */
  def Question2(textFile: RDD[Array[String]]): List[Int] = {
  
    // Convert into an RDD tuple of PostId (key) and VoteTypeId
    val convertToTuple = textFile.map( x => (x(PostId).toInt, x(VoteTypeId).toInt ) )
    // Filter out only the favourite votes
    val favVotes =  convertToTuple.filter(x => x._2 == 5)
    // Count how many post are ascociated with each VoteTypeId and construct a list of all the post
    val totalsByVoteTypeId = favVotes.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1, x._2 + y._2))
    // Filter out the ones that don't have a count value > 5
    val greaterThanFive = totalsByVoteTypeId.filter( x => x._2._2 > 5)
    // Sort in descending order
    val sortedResults = greaterThanFive.sortByKey(false)
    
    // Sort the resulting PostIds in descending order and convert it into a list of Ints as specified
    val firstCol : List[Int] = sortedResults.map( x => (x._1) ).collect().toList
    
    return firstCol
  }


  
  def main(args: Array[String]) {
    val inputFile = args(0)    
    val conf = new SparkConf().setAppName("Ass3").setMaster("local")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile).map(_.split(","))
    val votetype = Question1(textFile)
    println(votetype)
    val posts = Question2(textFile)
    println(posts)
  }
}
