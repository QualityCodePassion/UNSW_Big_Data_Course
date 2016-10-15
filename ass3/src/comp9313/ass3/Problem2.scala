package comp9313.ass3

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

object Problem2 {
  
  /** Code for Problem 2:
  *  input - RDD(String) of the words
  *  returns - "the average length of words starting with each letter. This
  *  means that for every letter, you need to compute: the total length 
  *  of all words that start with that letter divided by the total number 
  *  of words that start with that letter"
  */
  def CalcAvgWordLength(voteTuple: RDD[(String)]) : RDD[(Char,Double)] = {
    // Step 1: Get the first letter of each word and make it the key
    // (convert to lower case and filter out non-alpha chars)
    val filteredWords = voteTuple.filter( x => x.isEmpty() != true )
    val mapByFirstLetter = filteredWords.map(x => (x.charAt(0).toLower, (x.length().toFloat, 1.0)) ).filter( x => x._1.isLetter )
    
    // Step 2: Calculate:
    // wordLengthForKey = "total length of all words that start with that letter"
    // wordCountForKey = "total number of words that start with that letter"
    val totals = mapByFirstLetter.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2) )  

    // Step 3: Calculate the ave and print out the key value pairs
    val avg = totals.map( x => (x._1, x._2._1/x._2._2 ) ).sortByKey(true)
    
    // Print the resullts to the console
    for(temp<-avg)
    {
      println(temp._1 + "\t" + temp._2)
    }
    
    return avg
  }



  
  def main(args: Array[String]) {
    val inputFile = args(0)    
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("Problem2").setMaster("local")
    val sc = new SparkContext(conf)

    // Load the data and extract the words using regular expressions
    val textFile = sc.textFile(inputFile)
    val words = textFile.flatMap(line => line.split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+"))
    
    val output = CalcAvgWordLength(words)
    
    // Convert to string and write to the output file
    output.map(x => x._1 + "\t" + x._2).saveAsTextFile(outputFile)
    println("Output written to file in: " + outputFile )
  }
}
