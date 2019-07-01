import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {

  def map(line: String): Array[(Int, Int)] = {
    line.split(",", -1)
    .zipWithIndex
    .drop(1)
    .filter(_._1 != "")
    .map(pair => (pair._2, 1))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val output = textFile.flatMap(line => map(line))
    .groupByKey()
    .map{v => v._1 + "," + v._2.size};
    
    output.saveAsTextFile(args(1))
  }
}
