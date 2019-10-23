package gov.pnnl.stm.utilities.mag
import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

import scala.io.Source

object getPerYearIntPaper {

  def main(args: Array[String]): Unit = {

    val perYearPaperDir = args(0) // A
    val separator = '\t'.toString
    val perYearIntPaperDir = args(1) // 101
    val perYearIntPaperKeyDir = args(2) // A,101

    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("MAG")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // get yearly paper, lets do it serially.
    val yearArr = 1990 to 2017 toArray

    var globalPaperMap : scala.collection.mutable.Map[Int,String]
      = scala.collection.mutable.Map.empty
    yearArr.foreach(year => {
      val yearlyPaper: Map[String, Int] = Source
        .fromFile(perYearPaperDir + "/" + year + ".txt")
        .getLines()
        .map(paper => (paper.trim, 1))
        .toMap

    })
      ////NOT COMPLETE
    }

}
