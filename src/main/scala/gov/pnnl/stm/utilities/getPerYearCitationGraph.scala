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



    // get yearly paper, lets do it serially.
    val yearArr = 1990 to 2017 toArray

    /*
     * https://stackoverflow.com/questions/10157487/using-a-char-array-as-hashtable-key
     * Can not use char array as the key. so using string instead
     */
    yearArr.foreach(year => {
      val yearlyPaper: Map[String, Int] = Source
        .fromFile(perYearPaperDir + "/" + year + ".txt")
        .getLines()
        .map(paper => (paper.trim , 1)).toMap

      println("year:numPaper", year, yearlyPaper.size)
      println("Time:", System.nanoTime())
      // broadcast it to all executors
      val localPaperMap = sc.broadcast(yearlyPaper).value

      println("year:numPaper Broadcast", year, localPaperMap.size)
      println("Time:", System.nanoTime())
      // in parallel load only the reference paper from this year
      val thisYearRefer = sc.textFile(paperRefFile).mapPartitionsWithIndex((pid,localdata) =>{
        println("local parti id ", pid)
        println("local map size", localPaperMap.size)
        localdata.filter(line => localPaperMap.contains(line.split(separator)(0).trim))
          .map(line => {
            val pap_refPaper = line.split(separator)
            (pap_refPaper(0),pap_refPaper(1))
          })
      }).cache()
      println("This year reference ", thisYearRefer.count())
      println("Time:", System.nanoTime())


      //get output file, collect the ciation graph and save
      val op = new PrintWriter(new File(outFilepath + "/" + year + ".txt"))
      thisYearRefer.collect.foreach(
                                     pap_refPaper => op.println(pap_refPaper._1 + "," + pap_refPaper._2)
                                   )

      op.flush()
      thisYearRefer.unpersist(true)

    })
  }

}
