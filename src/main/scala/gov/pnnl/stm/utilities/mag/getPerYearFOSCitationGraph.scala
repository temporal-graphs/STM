package gov.pnnl.stm.utilities.mag
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

import scala.io.Source

object getPerYearFOSCitationGraph {

  def main(args: Array[String]): Unit = {

    val perYearCitationDir = args(0) // A
    val separator = '\t'.toString
    val paperFOSFile = args(1) // FOS_X
    val perYearFOSCitationDir = args(2)

    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("MAG").setMaster("local")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // get yearly paper, lets do it serially.
    val yearArr = 1990 to 2017 toArray

    yearArr.foreach(year => {
      val yearlyCitation: Map[String, String] = Source.fromFile(perYearCitationDir + "/" + year + ".txt")
        .getLines()
        .map(paper => (paper.split(",")(0).trim, paper.split(",")(1).trim))
        .toMap

      val yearlyPaperFromCitation: Map[String, Int] = Source.fromFile(perYearCitationDir + "/" + year + ".txt")
        .getLines()
        .flatMap(
          paper =>
            Array((paper.split(",")(0).trim, 1), (paper.split(",")(1).trim, 1))
        )
        .toMap
      
      // broadcast it to all executors
      val localCitationMap = sc.broadcast(yearlyCitation).value
      val localPaperMap = sc.broadcast(yearlyPaperFromCitation).value

      val thisYearFOS = sc
        .textFile(paperFOSFile)
        .mapPartitionsWithIndex((pid, localdata) => {
          //println("local map size", localCitationMap.size)
          localdata
            .filter(
              line => localPaperMap.contains(line.split(separator)(0).trim)
            )
            .map(line => {
              val pap_refPaper = line.split(separator)
              (pap_refPaper(0), pap_refPaper(2))
            })
        })
        .cache()

      val driverThisYearFOS: Map[String, String] = thisYearFOS.collect().toMap
      println("Driver count of this year FOS MAP size", year,driverThisYearFOS.size)

      val op =
        new PrintWriter(new File(perYearFOSCitationDir + "/" + year + ".txt"))
      yearlyCitation.foreach(
        entry =>
          if(driverThisYearFOS.contains(entry._1) && driverThisYearFOS.contains(entry._2))
            op.println(
            driverThisYearFOS.getOrElse(entry._1, "PID_" + entry._1) + ",0," +
              driverThisYearFOS.getOrElse(entry._2, "PID_") + "," + year
        )
      )
      op.flush()
    })
  }

}
