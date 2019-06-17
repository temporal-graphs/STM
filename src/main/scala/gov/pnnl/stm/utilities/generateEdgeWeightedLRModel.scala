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
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author puro755
 *
 */
object generateEdgeWeightedLRModel {

  def main(args: Array[String]): Unit = {

    val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
    val sc = SparkContextInitializer.getSparkContext(sparkConf)
    	
    //val filename = "/Volumes/Projects/Darpa-SDGG/SocialNetwork/Real-World/tweet_graph_small.csv"
    val filename = "/Volumes/Darpa-SDGG/Procurement/PIERS_RealWorld/FMAIN_to_HS_to_MAIN_ALL_Full.edges"
    //val filename = "/Volumes/Projects/Darpa-SDGG/Communications/Gradient_RealWorld_Learning/employee_edges_phone_edgelist.csv"
     
     // val modelname = "scalaLinearRegressionWithSGDModelSocialPercentileDeg"
   //val modelname = "scalaLinearRegressionWithSGDModelCommunicationPercentileDeg"
      val modelname = "scalaLinearRegressionWithSGDModelProcurement"
      
    val graph = GraphLoader.edgeListFile(sc,filename)
    println("graph size in v is " , graph.vertices.count)
    println("graph size in e is " , graph.edges.count)
    
    /*
     * ((3949,4258),10)
				((20,8179),4)
				((124,10760),3)
				((3949,4244),2)
				((8717,9122),2)
				((4545,4582),4)
				((4805,5246),65)
     */
    val multiGraph = graph.subgraph(triple=>triple.srcId != triple.dstId)
    val multiEdges = multiGraph.triplets.map(triple
        =>((triple.srcId , triple.dstId), 1)).reduceByKey((cnt1, cnt2) => cnt1+cnt2)

    /*
         * for social mention graph it is a power low 
         * so we get 95 percentile value
         * max is 445
         * average is 3.1
         * median is 1
         * 95 percentile is 10
         * 
         * after removing self edges
         * max is of is 206
         * cutt of is 10.0
         */

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    val df = multiEdges.map(row => row._2.toLong).toDF
    df.registerTempTable("table")
    val multiEdgeCutOff = sqlContext.sql("SELECT PERCENTILE(value, 0.90) FROM table").first().getDouble(0)
    println("cutt of is " + multiEdgeCutOff)
    val max = multiEdges.map(f=>f._2).max
    println("max is of is " + max)
    
    /*
     * +---------------------------------------+
				|percentile(value, CAST(0.95 AS DOUBLE))|
				+---------------------------------------+
				|                                   10.0|
				+---------------------------------------+
				
				+---------------------------------------+
				|percentile(value, CAST(0.90 AS DOUBLE))|
				+---------------------------------------+
				|                                   6.0|
				+---------------------------------------+
				+---------------------------------------+
				|percentile(value, CAST(0.75 AS DOUBLE))|
				+---------------------------------------+
				|                                   2.0|
				+---------------------------------------+

     */
    val mENorm = multiEdges.map(edge=>(edge._1, edge._2.toDouble/multiEdgeCutOff))
    val mENormScalled = mENorm.map(f=>{
      if(f._2 > 1.0)
        (f._1, 1.0)
      else
          f
    })
    //mENormScalled.filter(f=>f._2>.6).collect.foreach(f=>println(f._1, f._2))
    /* one node pair mentions itself which we have removed
     * ((2575,2575),1.0)
     * 
     * Rest have low value
     * ((5185,5271),1.0)
				((922,1355),0.8)
				((20,7833),1.0)
				((2575,8),0.8)
				((143,1148),1.0)
				((208,245),1.0)
				((3949,4078),1.0)
     */
    
    // Now get max degree of the graph
    // UPDATE: V3 of data: Instead of max degree, get a percentile value (eg 90) 
    val grpahDegRDD = multiGraph.degrees.map(v=>v._2.toLong)
    grpahDegRDD.toDF.registerTempTable("table")
    val degCutOff = sqlContext.sql("SELECT PERCENTILE(value, 0.90) FROM table").first().getDouble(0)

    val vAvgDeg = multiGraph.degrees.map(v=>(v._1, v._2.toDouble/degCutOff))
    
    //get SrcRDD from mENormScalled (v1ID, (v2ID, v1v2Wt))
    val srcRDD = mENormScalled.map(mEdge => (mEdge._1._1, (mEdge._1._2, mEdge._2)))
    
    //Join srcRDD and vAvgDeg to get src's normalized degree instead of its ID
    // (v1ID, ((v2ID , v1v2Wt), v1NormDeg))
    val srcNormDegRDD = srcRDD.join(vAvgDeg)
    
    //Now get Dest RDD to gets its avgDeg. entry = (v1ID, ((v2ID , v1v2Wt), v1NormDeg))
    // We get (V2ID, (V1NormDeg, v1v2Wt))
    val dstRDD = srcNormDegRDD.map(entry=>(entry._2._1._1, (entry._2._2, entry._2._1._2)))
    
    //Join dstRDD and vAvgDeg to get dst's normalized degree instead of its ID
    // Now we have (V2ID, ((V1NormDeg, v1v2Wt) , v2NormDeg))
    val dstNormDegRDD = dstRDD.join(vAvgDeg)
    
    //Now get the clean RDD to be used to create Training dataset
    // We get (v1NormDeg, v2NormDeg, v1v2Wt)
    val trainingRDD = dstNormDegRDD.map(entry=>(entry._2._1._1, entry._2._2, entry._2._1._2))
    
    //Create Training data for linear Regression
    val trainingData = trainingRDD.map(point=>
      {
        var featureArray :Array[Double] = new Array(2)
        featureArray(0) = point._1.toDouble
        featureArray(1) = point._2.toDouble
        LabeledPoint(point._3.toDouble, Vectors.dense(featureArray))
        
      })

    // Building the model
    val numIterations = 100
    val stepSize = 0.00000001
    val model = LinearRegressionWithSGD.train(trainingData, numIterations, stepSize)
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = trainingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, modelname)
    
    
    
  }

}