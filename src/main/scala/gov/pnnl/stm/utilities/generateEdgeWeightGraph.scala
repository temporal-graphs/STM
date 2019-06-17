/**
 *
 * @author puro755
 * @dJul 26, 2017
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import java.io.PrintWriter
import java.io.File
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionModel
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.graphx.Graph.graphToGraphOps

/**
 * @author puro755
 *
 */
object generateEdgeWeightGraph {

  def main(args: Array[String]): Unit = {

    val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)
     
    //val sameModel = LinearRegressionModel.load(sc, "scalaLinearRegressionWithSGDModelCommunication")
    //val sameModel = LinearRegressionModel.load(sc, "scalaLinearRegressionWithSGDModelCommunicationPercentileDeg")
    //val sameModel = LinearRegressionModel.load(sc, "scalaLinearRegressionWithSGDModelSocialPercentileDeg")
    val sameModel = LinearRegressionModel.load(sc, "scalaLinearRegressionWithSGDModelProcurement")
    
    println("done")
    
    //val filename = "/Volumes/Projects/Darpa-SDGG/SocialNetwork/Synthetic/100KNodes/V1/tweet_graph_small_kron_i17.edges"
    //val filename = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V1/FMAIN_to_HS_to_MAIN_ALL_CO_HS_i17.edges"
    //val filename = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V1/FMAIN_to_HS_to_MAIN_ALL_HS_CO_i17.edges"
    //val filename = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V2/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_i17.edges"
    //val filename = "/Volumes/Darpa-SDGG/SocialNetwork/Synthetic/100KNodes/V4/tweet_graph_small_kron_i17_g2.edges"
    //val filename = "/Volumes/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V4/gradient_kron_phone_i17_g2.edges"
    //val filename = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_i17.edges"
    //val filename = "/Volumes/Projects/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V1/gradient_kron_phone_i17.edges"
    //val filename = "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i13_oct.edges"
    //val filename = "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i16_oct.edges"
    //val filename = "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i17_oct.edges"
    val filename = "/Users/puro755/OctRelease/gradient_kron_phone_i13_oct.edges"
    //val filename = "/Users/puro755/OctRelease/gradient_kron_phone_i17_oct.edges"
    //val filename = "/Users/puro755/OctRelease/ALL_CO_HS_CO_i13_oct.edges"
    //val filename = "/Users/puro755/OctRelease/ALL_CO_HS_CO_i17_oct.edges"
    //val outfile =  "/Volumes/Projects/Darpa-SDGG/SocialNetwork/Synthetic/100KNodes/V3/tweet_graph_small_kron_i17.edges"
    //val outfile =  "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V2/FMAIN_to_HS_to_MAIN_ALL_CO_HS_i17.edges"
    //val outfile =  "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V2/FMAIN_to_HS_to_MAIN_ALL_HS_CO_i17.edges"
    //val outfile = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V2/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_Wt_i17.edges"
    //val outfile = "/Volumes/Darpa-SDGG/SocialNetwork/Synthetic/100KNodes/V4/tweet_graph_small_kron_i17_g2_Wt.edges"
    //val outfile = "/Volumes/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V4/gradient_kron_phone_i17_g2_Wt.edges"
    //val outfile = "/Volumes/Darpa-SDGG/Procurement/PIERS_Synthatic/100KNodes/V4/FMAIN_to_HS_to_MAIN_ALL_CO_HS_CO_Wt_i17.edges"
    //val outfile = "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i13_oct_wt.edges"
      //val outfile = "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i16_oct_wt.edges"
      //val outfile = "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i17_oct_wt.edges"
      val outfile = "/Users/puro755/OctRelease/gradient_kron_phone_i13_oct_wt.edges"
      //val outfile = "/Users/puro755/OctRelease/gradient_kron_phone_i17_oct_wt.edges"
      //val outfile = "/Users/puro755/OctRelease/ALL_CO_HS_CO_i13_oct_wt.edges"
      //val outfile = "/Users/puro755/OctRelease/ALL_CO_HS_CO_i17_oct_wt.edges"
    //val outfile = "/Volumes/Projects/Darpa-SDGG/Communications/Gradient_Synthetic/100KNodes/V3/gradient_kron_phone_i17.edges"
    val op = new PrintWriter(new File(outfile))
    
    val graph = GraphLoader.edgeListFile(sc,filename)
    
    val graphVertices = graph.vertices
    val grpahDegRDD = graph.degrees.map(v=>v._2.toLong)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    grpahDegRDD.toDF.registerTempTable("table")
    val degCutOff = sqlContext.sql("SELECT PERCENTILE(value, 0.90) FROM table").first().getDouble(0)
    val vAvgDeg = graph.degrees.map(v=>(v._1, v._2.toDouble/degCutOff))

    
    val avgDegreeGraph = graph.outerJoinVertices(vAvgDeg) { (id, oldAttr, deg) =>
    deg match {
    case Some(deg) => (oldAttr, deg)
    case None => (oldAttr, 0.0) // No degree means zero degree
    }
    }
    
    val predictRDD = avgDegreeGraph.triplets.map(triple=>(triple.srcId,triple.dstId,
        triple.srcAttr._2, triple.dstAttr._2))

    val weightedEdgeList = predictRDD.map(entry=>{
      var featureArray: Array[Double] = new Array(2)
      featureArray(0) = entry._3.toDouble
      featureArray(1) = entry._4.toDouble
      val prediction = sameModel.predict(Vectors.dense(featureArray))
      (entry._1, entry._2, prediction)
    })
    
    op.println("# Number of Nodes " + avgDegreeGraph.vertices.count)
    op.println("# Number of Edges " + weightedEdgeList.count)
    
    weightedEdgeList.collect.foreach(f=>op.println(f._1 + "\t" + f._2 + "\t" + f._3))
    
    op.flush()
  }

}