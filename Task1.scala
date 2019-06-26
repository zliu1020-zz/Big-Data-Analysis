import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(x => {
        var values = x.split(",", -1)
        var movieName = values(0)
        var max = -1
        var result = new StringBuilder

        for(i <- 1 until values.length){
            var rating = 0;
            if(values(i) != ""){
                rating = values(i).toInt
            }

            if(rating == max){
                result ++= "," + i.toString
            }else if(rating > max){
                max = rating
                result.clear()
                result ++= i.toString
            }
        }
        movieName + "," + result.toString
    })
    
    output.saveAsTextFile(args(1))
  }
}
