package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import gov.pnnl.builders.SparkContextInitializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import scalaz.Scalaz._

object getPerpWeaponTemporalG {

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

    val allValidFields = Array("GNAME", "WEAP")
    val validData = inputRDD.filter(event => {
      //println(event(3))

      allValidFields.contains(event(3).replaceAll("\"", "")) && (event(2)
        .contains("thisisNA") ==
        false) && (event(2).contains("Unknown") == false)
    })

    println("size of valid data " ,validData.count())

    val perpetratorAssociationG: RDD[(String, Map[String,Set[String]])] = validData
      .map(event => {
        if(event(3).contains("GNAME"))
          (event(1), Map("GNAME" -> Set(event(2))))
        else (event(1), Map("WEAP" -> Set(event(2))))
      })
      .reduceByKey((s1, s2) => s1 |+| s2)

    perpetratorAssociationG.collect.foreach(e=>println(e))

  val res = perpetratorAssociationG.filter(f=>f._2.size > 1)

    val op1 = new PrintWriter(new File("PerpWeapGraph.csv"))
    res.collect.foreach(e=>{
      val time = e._1.replaceAll("\"","").replaceAll("GTD_ID ","").substring(0,4)
      val allGrup = e._2.get("GNAME").get.toList
      val allWeap = e._2.get("WEAP").get.toList
      for(g <- allGrup)
        for(w <- allWeap)
          op1.println(g.replaceAll("\"","")
                        .replaceAll("GNAME","")+ ","
                      + w.replaceAll("\"","")
                        .replaceAll("WEAP","").split(":")(0).trim()
                      + "," + time)
    })
    op1.flush()
  }






}
