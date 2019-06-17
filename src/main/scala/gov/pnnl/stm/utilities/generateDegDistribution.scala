/**
 *
 * @author puro755
 * @dOct 9, 2017
 * @Mining
 */
package gov.pnnl.stm.utilities

import java.io.PrintWriter
import java.io.File
import org.apache.spark.graphx.GraphLoader
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author puro755
 *
 */
object generateDegDistribution {

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)

    val allFileNames = Array("/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i13_oct.edges",
       "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i16_oct.edges",
        "/Users/puro755/OctRelease/tmpOct/tweet_graph_small_kron_i17_oct.edges",
        "/Users/puro755/OctRelease/gradient_kron_phone_i13_oct.edges",
        "/Users/puro755/OctRelease/gradient_kron_phone_i16_oct.edges",
        "/Users/puro755/OctRelease/gradient_kron_phone_i17_oct.edges",
        "/Users/puro755/OctRelease/ALL_CO_HS_CO_i13_oct.edges",
        "/Users/puro755/OctRelease/ALL_CO_HS_CO_i16_oct.edges",
        "/Users/puro755/OctRelease/ALL_CO_HS_CO_i17_oct.edges"
        )
    for(filename <- allFileNames)
    {
      val outfile = filename + ".deg.tsv"
      val op = new PrintWriter(new File(outfile))
      val graph = GraphLoader.edgeListFile(sc, filename)
      val graphDegRDD = graph.degrees.map(v => (v._2.toLong, 1)).reduceByKey((cnt1,cnt2) => cnt1 + cnt2)sortBy(_._2,false)
      graphDegRDD.collect.foreach(f=>op.println(f._1 + "\t" + f._2))
      op.flush()
    }
     
    
       val allWtFiles = Array("/Users/puro755/OctRelease/OctReleaseWeighted/tweet_graph_small_kron_i13_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/tweet_graph_small_kron_i16_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/tweet_graph_small_kron_i17_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/gradient_kron_phone_i13_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/gradient_kron_phone_i16_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/gradient_kron_phone_i17_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/ALL_CO_HS_CO_i13_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/ALL_CO_HS_CO_i16_oct_wt.edges",
      "/Users/puro755/OctRelease/OctReleaseWeighted/ALL_CO_HS_CO_i17_oct_wt.edges"
    )
    
    val wtMinMaxFile = new PrintWriter(new File("/Users/puro755/OctRelease/OctReleaseWeighted/allFileWeightMaxMin.tsv"))
    for(wtFile <- allWtFiles)
    {
      println("File is " + wtFile)
      val wtRDD = sc.textFile(wtFile).filter(ln => !ln.startsWith("#")).map { 
        line => line.split("\t")(2).toDouble
      }
      val max = wtRDD.max
      val min = wtRDD.min
      wtMinMaxFile.println(wtFile.split("OctReleaseWeighted/")(1) + "\t" + max + "\t" + min)
    }
       wtMinMaxFile.flush()
  }

}