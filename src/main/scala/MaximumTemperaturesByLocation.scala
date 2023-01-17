import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.max

object MaximumTemperaturesByLocation {
  private def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    (fields(0), fields(2), fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc: SparkContext = new SparkContext("local[*]", "MaximumTemperaturesByLocation")
    val lines: RDD[String] = sc.textFile("data/1800.csv")
    val parsedLines: RDD[(String, String, Float)] = lines.map(parseLine)
    val minTemperatures: RDD[(String, String, Float)] = parsedLines.filter(x => x._2 == "TMAX")
    val stationTemperatures: RDD[(String, Float)] = minTemperatures.map(x => (x._1, x._3))
    val minimumTemperaturesByStation: RDD[(String, Float)] = stationTemperatures.reduceByKey((x, y) => max(x, y))
    val results: Array[(String, Float)] = minimumTemperaturesByStation.collect()

    results.sorted.foreach(x => println(s"${x._1} minimum temperature: " + f"${x._2}%.2f F"))
  }
}
