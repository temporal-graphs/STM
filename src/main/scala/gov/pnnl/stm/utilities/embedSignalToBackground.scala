/**
 *
 * @author puro755
 * @dAug 10, 2017
 * @Mining
 */
package gov.pnnl.stm.utilities
import java.io.PrintWriter
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.Random
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.Array.canBuildFrom

/**
 * @author puro755
 *
 */
object embedSignalToBackground {

  val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
  val sc = SparkContextInitializer.getSparkContext(sparkConf)
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

    
  def main(args: Array[String]): Unit = {
    
    ///Volumes/Darpa-SDGG-2/SocialNetwork/Synthetic/100KNodes/V4-Viz/tweet_graph_small_kron_i17_wSignalLowWtShiftZero.edges
    // this is the new C1-Sig.edges file
    var backgroundfilename = "/Volumes/Darpa-SDGG-2/SocialNetwork/Synthetic/100KNodes/V3/tweet_graph_small_kron_i17.edges"
    var signalFilename = "/Volumes/Darpa-SDGG-2/SignalGraphs/SM_graph0To64.edges"
    var outputfileName = "/Volumes/Darpa-SDGG-2/SocialNetwork/Synthetic/100KNodes/V4-Viz/tweet_graph_small_kron_i17_wSignalLowWtShiftZero.edges"
    
    //serializeCombinedGraph(backgroundfilename,signalFilename,outputfileName)
    
    backgroundfilename = "/Volumes/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V3/gradient_kron_phone_i17.edges"
    signalFilename = "/Volumes/Darpa-SDGG/SignalGraphs/Comms_graph.edge"
    outputfileName = "/Volumes/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V4/gradient_kron_phone_i17_wSignalLowWt.edges"
      
    //serializeCombinedGraph(backgroundfilename,signalFilename,outputfileName)
    
    backgroundfilename = "/Volumes/Darpa-SDGG-2/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_Wt_i17.edges"
    signalFilename = "/Volumes/Darpa-SDGG-2/SignalGraphs/Proc_graphOffset.edges"
    outputfileName = "/Volumes/Darpa-SDGG-2/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_Wt_i17_sSignalLowWtOffsetForVizOnly.edges"
      
    serializeCombinedGraph(backgroundfilename,signalFilename,outputfileName)
    // for this graph, no wt is specified so needs custom 
    
  }
  
  def serializeCombinedGraph(backgroundfilename: String, signalFilename: String, outputfileName : String)
  {
    
    val op = new PrintWriter(new File(outputfileName))
    val bgEdges = sc.textFile(backgroundfilename).filter(line => !line.startsWith("#")).map(line => {
      val lineArr = line.split("\t")
      (lineArr(0).toLong, lineArr(1).toLong, lineArr(2).toDouble)
    })
    val bgGraphSize = bgEdges.flatMap(e => {
      Set(e._1, e._2)
    }).distinct

    val signalEdges = sc.textFile(signalFilename).map(line => {
      val lineArr = line.split(" ")
      (lineArr(0).toLong, lineArr(1).toLong, lineArr(3).replaceAll("}", "").toDouble)
    })

    val allSignalVertices = signalEdges.flatMap(e => {
      Set(e._1, e._2)
    }).distinct.collect

    val localsignalEdges = signalEdges.map(se=>(se._1, se._2)).collect
    
    //val allSignalVCount = allSignalVertices.count
    
    /*
     * get new background graph with updated node id
     */
    val combinedEdges = bgEdges.union(signalEdges)
    
    // join edges by getting only the max weight
    val finalEdges = combinedEdges.map(ce => ((ce._1, ce._2), ce._3)).reduceByKey((wt1, wt2) => Math.max(wt1, wt2))
    println("bg size", bgEdges.count)
    println("combined size ", combinedEdges.count)
    println("final size ", finalEdges.count)
    //finalEdges.collect.map(e=>op.println(e._1._1 + "\t" + e._1._2 + "\t" + e._2))
    //op.flush()
    val rn = new Random()
        op.println("Source\tTarget\tDw\tWeight")
    finalEdges.collect.map(e=>
      if( localsignalEdges contains (e._1._1,e._1._2))
      		op.println(e._1._1 + "\t" + e._1._2 + "\t" + e._2+ "\t1" )
        	//skip = true
/*      else
        if ((allSignalVertices contains e._1._1) && (allSignalVertices contains e._1._2))
          skip=true
*/	        	//op.println(e._1._1 + "\t" + e._1._2 + "\t" + e._2+ "\t0" ))
	      else if((allSignalVertices contains e._1._1) )//|| (allSignalVertices contains e._1._2))
	        op.println(e._1._1 + "\t" + e._1._2 + "\t" + e._2+ "\t0" )
	        else if(rn.nextInt(100) < 1) //else if(rn.nextInt(100) < 10) is used for Finally used in gradient_kron_phone_i17_wSignalLowWtBGOnly50 file
	          op.println(e._1._1 + "\t" + e._1._2 + "\t" + e._2+ "\t0" ))
    op.flush()
  }

}