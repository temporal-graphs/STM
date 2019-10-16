package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

object getPerYearCitationGraph {

  def main(args: Array[String]): Unit = {

    val perYearPaperDir = args(0)
    val paperRefSummaryFile = args(1)
    val separator = '\t'.toString
    val outFilepath = args(2)
    val op = new PrintWriter(new File(outFilepath))

    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("MAG")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /*
     * create a Map of paper and its references. Broadcast it
     * Use scala local read instead of spark
     */
    import scala.io.Source
    val paperRefMap : scala.collection.mutable.Map[String,String] = scala.collection.mutable.Map.empty
    for (line <- Source.fromFile(paperRefSummaryFile).getLines) {
      val firstComma = line.indexOf(',')
      val paper = line.substring(0,firstComma)
      val allRefs = line.substring(firstComma + 1)
      paperRefMap += (paper -> allRefs)
    }

    val localpaperRefMap = sc.broadcast(paperRefMap).value
    println("Size of paper reference Map is ", paperRefMap.size)

    val yearArr = 1990 to 2017 toArray
    val yearRDD = sc.parallelize(yearArr)

    // See if this work as it is a map that return empty iterator
    yearRDD.mapPartitionsWithIndex((pid,localdata) => {

      localdata.foreach(year=>{
        val op = new PrintWriter(new File(outFilepath + "/" + year + ".txt"))
        for(paperStr <- Source.fromFile(perYearPaperDir + "/" + year + ".txt").getLines()){
         val allRefs = localpaperRefMap.getOrElse(paperStr, "").split(",")
          for(ref <- allRefs)
            { op.println(paperStr + "," + ref) }
         op.flush()
        }
      })
      Iterator.empty
    })
  }

}
