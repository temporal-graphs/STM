/**
 *
 * @author puro755
 * @dSep 4, 2017
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

/**
 * @author puro755
 *
 */
object createCompositeGraph {


  val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
  val sc = SparkContextInitializer.getSparkContext(sparkConf)

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def main(args: Array[String]): Unit = {
    
    //1 get top 2-11 degree nodes
    val JustinBNodeID = 64
    val c1sig = "/Volumes/Darpa-SDGG-2/SocialNetwork/Synthetic/100KNodes/V4-Composite/Input/tweet_graph_small_kron_i17_ShiftZeroNoWt.edges"
    val c1sigGraph  = GraphLoader.edgeListFile(sc,c1sig)
    val degV = c1sigGraph.degrees.sortBy(_._2, false).collect
    for(i <- 1 until 11)
    {
      println(degV(i)._1, degV(i)._2)
    }
    
    
    // 2 get node id of largest connected component in comm signal graph.
    val c2sigOnly = "/Volumes/Darpa-SDGG-2/SocialNetwork/Synthetic/100KNodes/V4-Composite/Input/Comms_graphNoWt.edge"
    val c2sigOnlyGraph  = GraphLoader.edgeListFile(sc,c2sigOnly)
    /*val CC = c2sigOnlyGraph.connectedComponents.vertices
    CC.collect.foreach(v=>println(v._1, v._2))*/
    val Com1Center = 14
    val Com2Center = 34
    val Com3Center = 30
    val Com4Center = 19
    
    //Array of 80% nbr of largest community's 'center' (center is handpicked
    val EightyPerCentNbrCom1C = c2sigOnlyGraph.triplets.filter(t 
        => (t.srcId ==Com1Center) || (t.dstId == Com1Center) ).sample(false,.8).map(t=>
          if(t.srcId == Com1Center) t.dstId
          else
            t.srcId).distinct.collect
    
    val Com1CNbrCount = EightyPerCentNbrCom1C.length
    
    //Now get Com1CNbrCount number Nbr from Social graph's signal center.
    val FrdsOfJustinB = c1sigGraph.triplets.filter(t 
        => (t.srcId ==JustinBNodeID) || (t.dstId == JustinBNodeID) ).sample(false,.8).map(t=>
          if(t.srcId == JustinBNodeID) t.dstId
          else
            t.srcId).distinct.take(Com1CNbrCount)

    var MappingCommToSocial : scala.collection.mutable.Map[Long,Long] 
    =  scala.collection.mutable.Map((14,64))
    for(i <- 0 until Com1CNbrCount)
      MappingCommToSocial.put(EightyPerCentNbrCom1C(i), FrdsOfJustinB(i))
MappingCommToSocial.foreach(f=>println(f._1, f._2))
      val CommOffset = 129999  //size of social network graph with signal
      val newCommGraph = c2sigOnlyGraph.mapVertices((id,attr) =>
        if(MappingCommToSocial.contains(id)) {
          val newLable = MappingCommToSocial.getOrElse(id, -1L)
          newLable
        }else
          attr + CommOffset)
      // So now labels are aligned
      val op = new PrintWriter(new File("CompositeGraph.edges"))   
      
    
    // write Updated Communication Graph
    newCommGraph.triplets.collect.foreach(t=>op.println(t.srcAttr + "\t" + t.dstAttr))
    op.flush()
    //Now  udpate the Communication signal graph and change vertex indices to match with Social one.
   /* val newCommSiganlGraphEdges : RDD[(Long,Long)] = c2sigOnlyGraph.triplets.map(t 
        => if (MappingCommToSocial.contains(t.srcId)) {
      //Means this nodes need to be reindexed and match indexes of Social Graph
      (MappingCommToSocial.getOrElse(t.srcId, -1L), t.dstId)
    } else if (MappingCommToSocial.contains(t.dstId))
      (MappingCommToSocial.getOrElse(t.dstId,-1L), t.srcId)
    else
      (t.srcId, t.dstId))*/
   //val SocialPlusCommSignalV =   c1sigGraph.vertices.union(newCommSiganlGraphVertices) 
      val udpateCommSignalGrpah = c2sigOnlyGraph.mapVertices((id,attr) => {
        if(MappingCommToSocial.contains(id))  (MappingCommToSocial.getOrElse(id, -1L))
        else
          (id,attr)
      })
  }

  
}