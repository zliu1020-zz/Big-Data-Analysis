import org.apache.spark.{SparkContext, SparkConf}

object Task4 {

implicit class Crossable[X](xs: Traversable[X]) {
  def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
}

  def processPerLine(line: String) : Array[(Int, String, Int)] = {
  	var values = line.split(",").zipWithIndex
  	// return format is (userIndex, movieName, rating)
    var result : Array[(Int, String, Int)] = Array()
    var movieName = values(0)._1

  	for (i <- 1 until values.length) {
  		// filter out the blank ratings 
  		if(values(i)._1 != "") {
  			result = result :+ (values(i)._2, movieName, values(i)._1.toInt)
  		}
  	}
  	result
  } 



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // use flatMap to output the result
    // presist is used for optimazation
    val movieRatings = textFile.flatMap(line => processPerLine(line)).persist()

    // broadcast is used for map-side join
    // val broadcastVar = sc.broadcast(movieRatings.collect).value
    // movieRatings.saveAsTextFile(args(1))
    // val similarityData = broadcastVar.cartesian(broadcastVar).saveAsTextFile(args(1))

    val moviePairData = movieRatings.cartesian(movieRatings)
    								// only keep records which are in ascending lexicographic order
    								.filter{ case ((userA, movieA, ratingA), (userB, movieB, ratingB)) => movieA < movieB && userA == userB }
    								// calculate similarity, format (movieA, movieB, similarity)
                    				.map{ case ((userA, movieA, ratingA), (userB, movieB, ratingB)) => ( movieA + "," + movieB, 
                          				( if( userA == userB && ratingA == ratingB) 1 else 0 ))}
                    				// aggreate records by key (movieA, movieB)
                    				// a, b are similarityData
                    				// 1 stands for number of partition 
                    				.reduceByKey((a,b) => (a + b), 1)
                    				// format output
                    				.map{case (movies, similarityData) => movies + "," + similarityData}
    								.saveAsTextFile(args(1))
    // sc.parallelize(moviePairData).saveAsTextFile(args(1))
  }
}