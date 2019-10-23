package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}

object getLethalityGraph {

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("DARPA-MAA STM")
      .setMaster("local")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Get a dictionary of events with their lethality as #nkills
    val inputRDDNKill = sc
      .textFile(
                 "/Users/puro755/OneDrivePNNL/Projects/DARPA_MAA/Data/START_Graphs/GTD" +
                 "/New_and_Improved/gtd_1970_temporal_NODENkill.csv"
               )
      .map ( line =>
        line.split(",")
           ).filter(arr=>{
      //println(arr.toList)
      arr(0).contains("GTD_ID") && arr(1) != "NA"}).map(arr=>{
      //println(arr.toList)
      (arr(0),arr(1).replaceAll("\"","").toInt)
    }).collect().toMap

    println("size of map ", inputRDDNKill.size)


    // now get the group info from edges

    val inputRDD = sc
      .textFile(
                 "/Users/puro755/OneDrivePNNL/Projects/DARPA_MAA/Data/START_Graphs/GTD" +
                 "/New_and_Improved/gtd_1970_temporal_EDGE.csv"
               )
      .map { line =>
        line.split(",")
           }

    val allValidFields = Array("GNAME")
    val validData = inputRDD.filter(event => {
      //println(event(3))
      allValidFields.contains(event(3).replaceAll("\"", "")) && (event(2)
        .contains("thisisNA") ==
        false)
    }).map(event => (event(1),event(2), inputRDDNKill.getOrElse(event(1),-1)))

    val op = new PrintWriter(new File("LethalityGraphAll.csv"))
    validData
      .filter(e => e._3 > -1)
      .collect
      .foreach(
        e =>
          op.println(
            e._2.replaceAll("\"", "").replaceAll("GNAME ", "")
              + "," + e._1
              .replaceAll("\"", "")
              .replaceAll("GTD_ID ", "") + "," + e._3
        )
      )

    op.flush()
  }

}
