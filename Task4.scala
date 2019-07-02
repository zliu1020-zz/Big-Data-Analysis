import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashMap

object Task4 {
  var moviesCombination: HashMap[String, Boolean] = HashMap.empty[String, Boolean]

  def calculateSimilarity(currentLine: String, textFile: Array[String], currengLineIndex: Int) : Array[String] = {
    // need -1 in split function to split empty ratings
    var currentLineSplit = currentLine.split(",", -1)
    var similarity = 0;
    var result: Array[String] = Array()

    // loop the whole textFile from currengLineIndex
    for (i <- currengLineIndex until textFile.length) {
      // split each line
      var lineSplit = textFile(i).split(",", -1)

      var smallerMoive = currentLineSplit(0)
      var biggerMovie = lineSplit(0)

      // check and compare movies name
      // movies names should be in ascending order
      if (smallerMoive != "" && biggerMovie != "") {
        if (smallerMoive > biggerMovie) {
          smallerMoive = lineSplit(0)
          biggerMovie = currentLineSplit(0)
        }

        var combine = smallerMoive + "," + biggerMovie

        // check if the movie names equal to avoid self-compare
        // does not redo the similarity calculation if already done
        if ( (smallerMoive != biggerMovie) && !(moviesCombination.contains(combine)) ) {
          // loop from the first ratings and calculate similarity
          for (j <- 1 until lineSplit.length) {
            // does not count blank ratings
            if ((currentLineSplit(j) != "") && (lineSplit(j) != "")) {
              // increase the similarity if ratings are the same
              if (currentLineSplit(j) == lineSplit(j)) {
                similarity = similarity + 1
              }
            }
          }
          // stores the movie names combination
          moviesCombination += (combine -> true)
          // stores the result
          result = result :+ combine + "," + similarity
          // reset 
          similarity = 0
        }
      }
    }
    result
  }




  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))
    // broadcast input textFile
    val broadcastedTextFile = sc.broadcast(textFile.collect).value

    // pass currentLineIndex into the function
    val moviePairRatings = broadcastedTextFile.zipWithIndex.flatMap{case (line, index) => calculateSimilarity(line, broadcastedTextFile, index)}

    // 1 means the partition number is 1
    sc.parallelize(moviePairRatings, 1).saveAsTextFile(args(1))
    
  }
}