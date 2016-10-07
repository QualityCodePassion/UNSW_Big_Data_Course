package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Ass3 {

  var Id = 0
  var PostId = 1
  var VoteTypeId = 2
  var UserId = 3
  var CreationTime = 4

  def Question2(textFile: RDD[Array[String]]): List[Int] = {
	//write your code
  }

  def Question1(textFile: RDD[Array[String]]) : (String, Iterable[String]) = {
	//write your code
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
