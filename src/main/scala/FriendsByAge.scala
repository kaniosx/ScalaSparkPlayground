import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FriendsByAge {
  private def parseLine(line: String): (Int, Int) = {
    val fields = line.split(",")
    (fields(2).toInt, fields(3).toInt)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc: SparkContext = new SparkContext("local[*]", "FriendsByAge")
    val lines: RDD[String] = sc.textFile("data/fakefriends-noheader.csv")
    val rdd: RDD[(Int, Int)] = lines.map(parseLine)
    val totalsByAge: RDD[(Int, (Int, Int))] = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val averagesByAge: RDD[(Int, Int)] = totalsByAge.mapValues(x => x._1 / x._2)

    averagesByAge.collect().sortBy(x => x._1).foreach(println)
  }
}
