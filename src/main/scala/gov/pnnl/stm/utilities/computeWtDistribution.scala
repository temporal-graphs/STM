/**
 *
 * @author puro755
 * @dAug 10, 2017
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.graphx.GraphLoader
import java.io.PrintWriter
import java.io.File
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author puro755
 *
 */
object computeWtDistribution {

  val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
  val sc = SparkContextInitializer.getSparkContext(sparkConf)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]): Unit = {
    
//    val cohsG = GraphLoader.edgeListFile(sc,"/Volumes/Darpa-SDGG/15Aug2017Deliverables/C1-sig.edges")
//    println("number of vertices 1" + cohsG.vertices.count)
//    println("edges of vertices 1" + cohsG.edges.count)
//    
//    val cohsG2 = GraphLoader.edgeListFile(sc,"/Volumes/Darpa-SDGG/15Aug2017Deliverables/C2-sig.edges")
//    println("number of vertices 2" + cohsG2.vertices.count)
//    println("edges of vertices2 " + cohsG2.edges.count)
//    
//    
    val cohsG3 = GraphLoader.edgeListFile(sc,"/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3-sig.edges")
    println("number of vertices 3 " + cohsG3.vertices.count)
    println("edges of vertices 3" + cohsG3.edges.count)
    
    System.exit(1)
    
    var backgroundfilename = "/Volumes/Darpa-SDGG/SocialNetwork/Synthetic/100KNodes/V3/tweet_graph_small_kron_i17.edges"
    var outputfileName = "/Volumes/Darpa-SDGG/SocialNetwork/Synthetic/100KNodes/V3/tweet_graph_small_kron_i17_DegWtDistribution.csv"
    generateDist(backgroundfilename,outputfileName)    
      
    backgroundfilename = "/Volumes/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V3/gradient_kron_phone_i17.edges"
    outputfileName = "/Volumes/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V3/gradient_kron_phone_i17_DegWtDistribution.csv"
    generateDist(backgroundfilename,outputfileName)    
      
    backgroundfilename = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V2/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_Wt_i17.edges"
    outputfileName = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V2/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_Wt_i17_DegWtDistribution.csv"
    generateDist(backgroundfilename,outputfileName)
  }

  def generateDist(backgroundfilename:String, outputfileName:String)
  {
    val op = new PrintWriter(new File(outputfileName))
    
    val bgEdges = sc.textFile(backgroundfilename).filter(line => !line.startsWith("#")).map(line => {
      val lineArr = line.split("\t")
      (lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toDouble)
    })
    
    val allWtPerVertex = bgEdges.flatMap(e=>{
      Set((e._1, List(e._3)), (e._2, List(e._3)))
    }).reduceByKey((l1,l2) => l1:::l2)
    
    val avgWtPerVertex = allWtPerVertex.map(v=>(v._1, v._2.sum/v._2.size))
    
    val edgeListGraph = GraphLoader.edgeListFile(sc,backgroundfilename)
    val vDegree = edgeListGraph.degrees
    
    val avgWtDegPerVertex = avgWtPerVertex.join(vDegree)
    
    val allWtPerDegree = avgWtDegPerVertex.map(v=>(v._2._2, List(v._2._1))).reduceByKey((l1,l2) => l1:::l2)
    
    val avgWtPerDeg = allWtPerDegree.map(v=>(v._1, v._2.sum/v._2.size))
    
    avgWtPerDeg.collect.foreach(f=>op.println(f._1 +"\t" +f._2))
    op.flush()

  }
}