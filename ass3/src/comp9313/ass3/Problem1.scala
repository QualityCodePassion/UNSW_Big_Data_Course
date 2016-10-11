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
  def q1parseLine(line: String) = {
    val fields = line.split(",")
    val voteTypeId = fields(VoteTypeId).toInt
    val postId = fields(PostId)
    // Create a tuple that is our result.
    (voteTypeId, postId)
  }

  
  
  /** A function that splits a line of input into (postId, voteTypeId) tuples. */
  def q2parseLine(line: String) = {
    val fields = line.split(",")
    val voteTypeId = fields(VoteTypeId).toInt
    val postId = fields(PostId).toInt
    // Create a tuple that is our result.
    (postId, voteTypeId)
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

  
  def Question2(voteTuple: RDD[(Int,Int)]) : List[Int] = {
	  //write your code
   
    // Filter out only the favourite votes
    val favVotes =  voteTuple.filter(x => x._2 == 5)
    
    // Count how many post are ascociated with each VoteTypeId and construct a list of all the post
    val totalsByVoteTypeId = favVotes.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1, x._2 + y._2))
    // Filter out the ones that don't have a count value > 5
    val greaterThanFive = totalsByVoteTypeId.filter( x => x._2._2 > 5)
    // Sord in descending order
    val sortedResults = greaterThanFive.sortByKey(false)  //takeOrdered(0)(Ordering[Int].reverse.on(_))
    
    // Print out the result in the " List" format requested
    val firstCol = sortedResults.map( x => (x._1) )
    var isFirstVal = true
    //firstCol.foreach(println )
    for(temp<-firstCol)
    {
      if( isFirstVal)
      {
        isFirstVal = false
        print("List(")
      }
      else
      {
        isFirstVal = false
        print(", ")
      }
      
      print(temp)
    }
    print( ")" )


    var buf = scala.collection.mutable.ListBuffer[Int]()

    /* TODO Will try an convert the RDD to a list if I get time
    
    // Obtain, then Sort and print the final results.
    //val buf = scala.collection.mutable.ListBuffer.empty[Int]

    for(temp<-firstCol)
    {
      println(temp.toInt)
      buf += temp.toInt //1 Not even having a constant works!
      println(buf)
    }
    
    // For some reason "buf" gets cleared!!!!
    println(buf)


    
    //var outList = scala.collection.mutable.Buffer[Int]()
    //for(str<-firstCol) outList += str
    val strArray = Array(1,2,3)
    var outList = scala.collection.mutable.ListBuffer[Int]()
    
    for(str<-strArray) // As soon as I change the "strArray" to the firstCol, it prints, but outList is empty?
    {
      outList += 1  //str.toInt
      println(str)
    }
    
    println(outList.toList)
    */
    
    return buf.toList  // This would work if buf wasn't empty!!
  }


  
  def main(args: Array[String]) {
    val inputFile = args(0)    
    val conf = new SparkConf().setAppName("Ass3").setMaster("local")
    val sc = new SparkContext(conf)

    //val textFile = sc.textFile(inputFile).map(_.split(","))
    val textFile = sc.textFile(inputFile)
    // Use our q1parseLines function to convert to (voteTypeId, postId) tuples
    val q1VoteTuple = textFile.map(q1parseLine)
    
    val votetype = Question1(q1VoteTuple)
    println(votetype)
    

    
    // Use our q2parseLines function to convert to (voteTypeId, postId) tuples
    val q2VoteTuple = textFile.map(q2parseLine)
    val posts = Question2(q2VoteTuple)
    //println(posts)
  }
}
