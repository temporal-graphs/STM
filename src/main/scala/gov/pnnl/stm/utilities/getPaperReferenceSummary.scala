package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

object getPaperReferenceSummary {

  def main(args: Array[String]): Unit = {

    val paperReferenceFile = args(0)
    val separator = '\t'.toString
    val outFilepath = args(2)
    val op = new PrintWriter(new File(outFilepath))

    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("MAG")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val paperReferenceSummary = sc.textFile(paperReferenceFile).map(line=>{
      val arr = line.split(separator)
      (arr(0),Array(arr(1)))
    }).reduceByKey((refArr1,refAff2) => refArr1 ++ refAff2).map(papAllRef => papAllRef._1
    + "," + papAllRef._2.mkString(","))

    paperReferenceSummary.collect.foreach(line=>op.println(line))
  }


}
