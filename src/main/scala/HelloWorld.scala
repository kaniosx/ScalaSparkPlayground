import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

object HelloWorld {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc: SparkContext = new SparkContext("local[*]", "HelloWorld")
    val lines: RDD[String] = sc.textFile("data/ml-100k/u.data")
    val numLines: Long = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

    sc.stop()
  }
}
