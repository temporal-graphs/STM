/**
 *
 * @author puro755
 * @dAug 2, 2017
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.spark.graphx.GraphLoader
import scala.util.Random
import java.io.PrintWriter
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import gov.pnnl.builders.SparkContextInitializer

/**
 * @author puro755
 *
 */
object mergePIERSBipartiteGraphs {

  def main(args: Array[String]): Unit = {

    val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //val CoHSFile = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V1/FMAIN_to_HS_to_MAIN_ALL_HS_CO_i17.edges"
    //val CoHSFile = "/Users/puro755/OctRelease/ALL_CO_HS_i13_oct.edges"
    //val CoHSFile = "/Users/puro755/OctRelease/ALL_CO_HS_i16_oct.edges"
    val CoHSFile = "/Users/puro755/OctRelease/ALL_CO_HS_i17_oct.edges"
    //val CoHSFile = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_HS_CO_i17_g2.edges"
    //val HSCOFile = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V1/FMAIN_to_HS_to_MAIN_ALL_CO_HS_i17.edges"
    //val HSCOFile = "/Users/puro755/OctRelease/ALL_HS_CO_i13_oct.edges"
    //val HSCOFile = "/Users/puro755/OctRelease/ALL_HS_CO_i16_oct.edges"
    val HSCOFile = "/Users/puro755/OctRelease/ALL_HS_CO_i17_oct.edges"
    //val HSCOFile = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_i17_g2.edges"
      
    //val outfile =  "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_i17.edges"
    //val outfile =  "/Users/puro755/OctRelease/ALL_CO_HS_CO_i13_oct.edges"
      /*
       * ( Total Offset by IDs,8191,8189,sum,16380)
				(7801,13055)
       */
      //val outfile =  "/Users/puro755/OctRelease/ALL_CO_HS_CO_i16_oct.edges"
        /*
         * ( Total Offset by IDs,65520,65522,sum,131042)
					(61712,116302)
         * 
         */
        val outfile =  "/Users/puro755/OctRelease/ALL_CO_HS_CO_i17_oct.edges"
          /*
           * ( Total Offset by IDs,131063,131055,sum,262118)
							(122962,241094)
           */
    //val outfile =  "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_i17_g2.edges"
    val op = new PrintWriter(new File(outfile)) 
      
    val cohsG = GraphLoader.edgeListFile(sc,CoHSFile)
    val hscoG = GraphLoader.edgeListFile(sc,HSCOFile)
    
    
    //instead of adding the count, we need to offset by sum of maxID in both the streams.
    val hsVCnt1 = cohsG.triplets.map(f=>f.dstId).distinct.max
    val hsVCnt2 = hscoG.triplets.map(f=>f.srcId).distinct.max
    val totalV = hsVCnt1 + hsVCnt2
    println(" Total Offset by IDs" , hsVCnt1, hsVCnt2 , "sum" , totalV)
    
    //Offset HS Vertex IDs, 
    /*
     * Now all HS IDS has no collision with Co IDs
     */
    val hs1EdgesList = cohsG.triplets.map(t=>(t.srcId, t.dstId))//we will fix in in the code below
    val hs2EdgesList = hscoG.triplets.map(t=>(t.srcId + totalV, t.dstId))
    println(hs1EdgesList.count, hs2EdgesList.count)
    
    val localUniqueHS2 = hs2EdgesList.map(t=>t._1).distinct.collect
    val localUniqueHS2Count = localUniqueHS2.length
    
    //we update the vertices ID of 'hs' in of co-hs graph, replace it with some value in update 'hs' in hs-co
    val udpatedhs1EdgeList = hs1EdgesList.map(e=>(e._1,localUniqueHS2(Random.nextInt(localUniqueHS2Count))))
    
    //IF if comnine edges form  udpatedhs1EdgeList, and   hs2EdgesList, thats our combined graph 
    val combinedEdges = udpatedhs1EdgeList.union(hs2EdgesList)
    
    //println(hs1EdgesList.count, hs2EdgesList.count, combinedEdges.count) //(241094,122962,364056)
    op.println("#Offset value is " + totalV)
    combinedEdges.collect.foreach(f=>op.println(f._1 + "\t" + f._2))
   
    
  }

}