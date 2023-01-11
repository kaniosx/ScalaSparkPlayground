import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object RatingsCounter {
  def main(array: Array[String]): Unit = {
    // set logger level to ERROR only
    Logger.getLogger("org").setLevel(Level.ERROR)

    // * to use all possible cores from local machine
    val sc: SparkContext = new SparkContext("local[*]", "RatingsCounter")

    val sortedResults: Seq[(String, Long)] = sc
      .textFile("data/ml-100k/u.data")
      .map(x => x.split("\t")(2))
      .countByValue()
      .toSeq
      .sortBy(_._1)

    sortedResults.foreach(println)
  }
}
