package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

object CreatePerpetrator {

  def main(args: Array[String]): Unit = {

    val sparkConf = SparkContextInitializer
      .getSparkConf()
      .setAppName("DARPA-MAA STM")
      .setMaster("local")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //val allPerpetrators = Array("GNAME", "gsubname2", "gsubname3")
    val allPerpetrators = Array("GNAME")
    val wtRDD = sc
      .textFile(
        "/Users/puro755/OneDrivePNNL/Projects/DARPA_MAA/Data/START_Graphs/GTD" +
          "/gtd_cbn_prep1_EDGE.csv"
      )
      .map { line =>
        line.split(",")
      }

    val validData = wtRDD.filter(event => {
      //println(event(3))
      allPerpetrators.contains(event(3).replaceAll("\"", "")) && (event(2)
        .contains("thisisNA") ==
        false)
    })
    println("valid data size is ", validData.count())

    val perpetratorAssociationG: RDD[(String, Set[String])] = validData
      .map(event => {
        (event(1), Set(event(2)))
      })
      .reduceByKey((s1, s2) => s1 ++ s2)

    val outG: RDD[(String, String, String)] =
      perpetratorAssociationG.flatMap(ev_Perpet => {
        var allPEdges: scala.collection.mutable.Set[(String, String, String)] =
          scala.collection.mutable.Set.empty
        val pArr = ev_Perpet._2.toList.toArray
        val totalPerpt = pArr.length

        //if(totalPerpt == 1)
          //allPEdges += ((pArr(0), pArr(0), ev_Perpet._1))
        for (i <- 0 to totalPerpt - 2) {
          for (j <- i + 1 to totalPerpt - 1)
            allPEdges += ((pArr(i), pArr(j), ev_Perpet._1))
        }
        allPEdges
      })

    val op = new PrintWriter(new File("PerteratorGraphGNAME.csv"))
    outG.collect.foreach(asso =>
      op.println(asso._1 + "," + asso._2 + "," + asso._3))

    op.flush()


    //1970-1975 graph
    val op1 = new PrintWriter(new File("PerteratorGraph1975.csv"))
    outG
      .filter(e => e._3.replaceAll("\"", "")
        .replaceAll("GTD_ID", "").trim().substring(0,4).toInt < 1976)
      .collect
      .foreach(asso => op1.println(asso._1 + "," + asso._2 + "," + asso._3))

    op1.flush()
  }

}
