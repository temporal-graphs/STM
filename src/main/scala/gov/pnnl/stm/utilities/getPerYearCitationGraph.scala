package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer

object getPerYearCitationGraph {

  def main(args: Array[String]): Unit = {

    val perYearPaperDir = args(0)
    val paperRefFile = args(1)
    val separator = '\t'.toString
    val outFilepath = args(2)

    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("MAG")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    /*
     * create a Map of paper and its references. Broadcast it
     * Use scala local read instead of spark
     *
     * 1. Map goes out of memory
     * 2. Char array rails while broadcasting: 34880393 total papers and
     * get  java.lang.ArrayIndexOutOfBoundsException: 1073741865
     *
     * So lets do this way:
     * 1. for every year, get all the papers
     * 2. broadcast that as array
     * 3. at every executor, only load reference graph for that year based on the broadcasted
     * array .
     * 4. save that year's reference graph
     */
    import scala.io.Source

    val allYearRefer = sc.textFile(paperRefFile).map(line=>{
      val linearr = line.split(separator)
      (linearr(0),linearr(1))
    }).cache()
    println("all year referecne size is ", allYearRefer.count())

    // get yearly paper, lets do it serially.
    val yearArr = 1990 to 2017 toArray

    yearArr.foreach(year => {
      val yearlyPaper: Array[String] = Source
        .fromFile(perYearPaperDir + "/" + year + ".txt")
        .getLines()
        .map(paper => paper)
        .toArray

      println("year:numPaper", year, yearlyPaper.length)
      // broadcast it to all executors
      val localPaperArr = sc.broadcast(yearlyPaper).value

      // in parallel load only the reference paper from this year
      val thisYearRefer = allYearRefer
        .filter(line => localPaperArr.contains(line._1))
        .map(pap_refPaper => (pap_refPaper._1, pap_refPaper._2)).cache()
      println("This year reference ", thisYearRefer.count())


      //get output file, collect the ciation graph and save
      val op = new PrintWriter(new File(outFilepath + "/" + year + ".txt"))
      thisYearRefer.collect.foreach(
        pap_refPaper => op.println(pap_refPaper._1 + "," + pap_refPaper._2)
      )

      op.flush()
      thisYearRefer.unpersist()

    })
//    val paperRefArrBuf : ArrayBuffer[(Array[Char],Array[Char])] = ArrayBuffer.empty
//    for (line <- Source.fromFile(paperRefSummaryFile).getLines) {
//      val firstComma = line.indexOf(',')
//      val paper = line.substring(0,firstComma)
//      val allRefs = line.substring(firstComma + 1).toCharArray
//      paperRefArrBuf += ((paper.toCharArray , allRefs))
//    }
//
//    println("Size of paper reference Map is ", paperRefArrBuf.size)
//    val localpaperRefMap :Map[Array[Char],Array[Char]] = sc.broadcast(paperRefArrBuf).value.toMap
//
//
//    val yearRDD = sc.parallelize(yearArr)
//
//    // See if this work as it is a map that return empty iterator
//    yearRDD.mapPartitionsWithIndex((pid,localdata) => {
//
//      localdata.foreach(year=>{
//        println(" pid is  , ", pid)
//        val op = new PrintWriter(new File(outFilepath + "/" + year + ".txt"))
//        for(paperStr <- Source.fromFile(perYearPaperDir + "/" + year + ".txt").getLines()){
//         val allRefs = localpaperRefMap.getOrElse(paperStr.toCharArray, "").toString.split(",")
//          for(ref <- allRefs)
//            { op.println(paperStr + "," + ref) }
//         op.flush()
//        }
//      })
//      Iterator.empty
//    })
  }

}
