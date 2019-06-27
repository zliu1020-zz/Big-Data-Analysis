import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def countPerLine(line: String) : (Int, Int) = {
  	var values = line.split(",", -1)
  	var sum = 0
  	for (i <- 1 until values.length) {
  		if(values(i) != "") {
  			sum = sum + 1
  		}
  	}
  	// the key is used for aggregation in reduceByKey function 
  	(0, sum)
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile
    	.map(line => countPerLine(line))
    	// the output is (0, 13) after reduceByKey
    	// limit number of partition to be 1
    	.reduceByKey(_+_, 1)
    	// output the second paramter in the tuple (0, 13)
    	.map(_._2)

    
    output.saveAsTextFile(args(1))
  }
}
