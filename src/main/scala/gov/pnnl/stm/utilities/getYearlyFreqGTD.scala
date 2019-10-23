package gov.pnnl.stm.utilities
import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

object getYearlyFreqGTD {

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("DARPA-MAA STM")
      .setMaster("local")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputRDD = sc
      .textFile(
                 "/Users/puro755/OneDrivePNNL/Projects/DARPA_MAA/Data/START_Graphs/GTD" +
                 "/gtd_cbn_prep1_EDGE.csv"
               )
      .map { line =>
        line.split(",")
           }

    val yearlyFreq = inputRDD.map(event=>{
      (event(1).replaceAll("\"","")
        .replaceAll("GTD_ID ", "").substring(0,4),1)
    }).reduceByKey((c1,c2) => c1 + c2)

    yearlyFreq.collect.foreach(y => println(y._1 + "," + y._2))
  }

}
