import org.apache.spark.{SparkContext, SparkConf}

object Task4 {

  def calculateSimilarity(currentLine: String, textFile: Array[String]) : Array[String] = {
    // need -1 in split function to split empty ratings
    var currentLineSplit = currentLine.split(",", -1)
    var similarity = 0;
    var result: Array[String] = Array()

    // loop the whole textFile
    for (i <- 0 until textFile.length) {
      // split each line
      var lineSplit = textFile(i).split(",", -1)

      var smallerMoive = currentLineSplit(0)
      var biggerMovie = lineSplit(0)

      // only calculate the similarity if in ascending order
      if ( smallerMoive < biggerMovie) {
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

        // stores the result
        result = result :+ smallerMoive + "," + biggerMovie + "," + similarity
        // reset 
        similarity = 0
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

    val moviePairRatings = broadcastedTextFile.flatMap(line => calculateSimilarity(line, broadcastedTextFile))

    // 1 means the partition number is 1
    sc.parallelize(moviePairRatings, 1).saveAsTextFile(args(1))
    
  }
}