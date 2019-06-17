/**
 *
 * @author puro755
 */
package gov.pnnl.stm.algorithms

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.graphframes.GraphFrame
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.ListBuffer
import java.io.PrintWriter
import java.io.File
import scala.collection.JavaConverters._
import java.util.ArrayList
import gov.pnnl.builders.SparkContextInitializer

/**
 * @author puro755
	*         Obsolete file..please see STM_NodArrivalRateMultiType even for single
	*         type motif analysis
 *
 */
object STM_NodeArrivalRate {

  val MInfo = scala.collection.mutable.ListBuffer.empty[List[Int]]
  val OffsetInfo = scala.collection.mutable.ListBuffer.empty[(Int,Int)]
  val t1=System.nanoTime()
  println("Type prefix_annotation without space: ")
  val scanner = new java.util.Scanner(System.in)
  val prefix_annotation = scanner.nextLine()
  val MProbFile = new PrintWriter(new File(t1+"MProbNOverlap_regenerated_"+prefix_annotation+".txt"))
  //val OffsetFile = new PrintWriter(new File(t1+"OffsetNOverlap_regenerated"+prefix_annotation+".txt"))
  //val MotifFile = new PrintWriter(new File(t1+"NonOverlappingMotif_regenerated"+prefix_annotation+".txt"))
  var localVAppearanceTime : scala.collection.immutable.Map[Int,Long] = scala.collection.immutable.Map.empty
	val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
	val sc = SparkContextInitializer.getSparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
  
  def main(args: Array[String]): Unit = {

    
    sc.getConf.getAll.foreach(println)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //val distinctTweetGraphTemporal = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_temporal_int_distint.csv"))
    //val duplicateTweetGraphTemporal = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_temporal_int_duplicate.csv"))
		val fileList = Array("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt500.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k20E.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt500_regenratedFromGSG.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt500_regenratedFromGSG_try2.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k_regenratedFromGSG_try1.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k_regenratedFromGSG_try2.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k_regenratedFromGSG_try3_a.3.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k_regenratedFromGSG_try5_fixedMotifPrefAttachNodeSelection.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/Data/gradient/tmp/intGraph/allPhoneInt20k_regenratedFromGSG_try6_fixedMotifRandomNodeSelection.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_temporal_int.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_temporal_int_timeOrder.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/twitterMentionTimeInSecSeedGraph_RegeneratedPrefAttchTry5.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/twitterMentionTimeInSecSeedGraph_RegeneratedRandomTry4.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/LowEndAutosSmallNoTypeInSeconds.csv",
			"/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/LowEndAutosSmallNoType_RegenTry2PrefAttach.3.csv")
    val nodeFile = fileList(2)
    val nodeQuadruples: RDD[(Int, Int,Long)] =
      sc.textFile(nodeFile).map { line =>
        var longtime = -1L
        val fields = line.split(" ")
        try{ 
            (fields(0).toInt, fields(1).toInt, fields(2).toLong)
        }catch{
          case ex: org.joda.time.IllegalFieldValueException => {
            println("IllegalFieldValueException exception")
            (-1,-1,-1L)
          }
          case ex: java.lang.ArrayIndexOutOfBoundsException =>
            {
              println("AIOB:", line)
              (-1,-1,-1L)
            }
          case ex: java.lang.NumberFormatException =>
            println("AIOB2:", line)
            (-1,-1,-1L)
        }
      }.distinct
      /*val nodeQuadruplesFrq = nodeQuadruples.map(f=>(f,1)).reduceByKey((cnt1,cnt2)=> cnt1+cnt2)
      val duplicate  = nodeQuadruplesFrq.filter(f=>f._2 > 1)
      duplicate.collect.foreach(f=>duplicateTweetGraphTemporal.println(f._1._1 + " " + f._1._2 + " " + f._1._3 ))
      duplicateTweetGraphTemporal.flush()
      val distint = nodeQuadruples.distinct.collect.foreach(f=>distinctTweetGraphTemporal.println(f._1 + " " + f._2 + " " + f._3 ))
      distinctTweetGraphTemporal.flush()
      System.exit(1)
    */println("nodeQuadruples siEZ is " , nodeQuadruples.count)
    
    val vRDD = nodeQuadruples.flatMap(nd=>Iterator((nd._1,nd._1),(nd._2,nd._2))).distinct
    
    /*
     * compute first appearance time of each vertex.
     * (vid,etime)===> get smallest etime for a give vid
     */
    val vAppearanceTime = nodeQuadruples.flatMap(nd
        =>Iterator((nd._1,nd._3),(nd._2,nd._3))).reduceByKey((time1,time2) => Math.min(time1, time2))
    //boradcaset it as array
    val vAppBroCa = sc.broadcast(vAppearanceTime.collect)
    localVAppearanceTime = vAppBroCa.value.toMap
    
    /*
     * Get total duration in seconds of input graph.
     */
    val allTimes = nodeQuadruples.flatMap(nd
        =>Iterator(nd._3,nd._3))
    val minTime = allTimes.min
    val maxTime = allTimes.max
    val duration = maxTime - minTime
    println("min time", minTime)
    println("max time", maxTime)
    println("duration", duration)
    import sqlContext.implicits._
    val vDF = vRDD.distinct.toDF("id", "name")
    println("number of vertex " , vDF.count)
    import sqlContext.implicits._
    val eDF = nodeQuadruples.distinct.toDF("src", "dst", "time")
    println("number of edges " , eDF.count)
    // Create a GraphFrame
    import org.graphframes.GraphFrame

		var g = GraphFrame(vDF, eDF)
		val atomocMotif = Array("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)",
			"(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(c)",
			"(a)-[e1]->(b); (a)-[e2]->(c)",
			"(b)-[e1]->(a); (c)-[e2]->(a)",
			"(a)-[e1]->(b); (b)-[e2]->(a)",
			"(a)-[e1]->(b)")
			import collection.JavaConversions._
         val degArray = g.toGraphX.degrees.values.collect.toList
         val vDegDist: java.util.ArrayList[Int] = new ArrayList(degArray.asJava)
      val  intList  = new java.util.ArrayList[Integer]()
			for (entry <- vDegDist)
			{
			    intList.add(entry)
			}
      //val exponent: Double = Discrete.fit(intList).fit().exponent();
        
    g = processUniqueMotif_3Edges(g, atomocMotif(0),true)
    g = processUniqueMotif_3Edges(g, atomocMotif(1),false)
    g = processUniqueMotif_2Edges(g, atomocMotif(2),true)
    g = processUniqueMotif_2Edges(g, atomocMotif(3),true)
    g = processUniqueMotif_2Edges(g, atomocMotif(4),true)
    g = processUniqueMotif_1Edges(g, atomocMotif(5))
    
    println(MInfo.toList)
    println("number of edges in last graph" ,g.edges.count)
    println("number of vertex in last graph" ,g.vertices.count)
    // Get arrival rates of each informative-motif 
    val MProb :ListBuffer[Double] = MInfo.flatMap(f0=>f0.map(f1 => f1.toDouble/duration))
    //lets assume 11 months data, 330 days, so 28510000 seconds
    MProbFile.println(MProb.mkString("\n"))
    MProbFile.println("duration="+duration)
    //OffsetFile.println(OffsetInfo.mkString(" ").replaceAll("\\(", "").replaceAll("\\)", "\n"))
    
    MProbFile.flush()
    //OffsetFile.flush()
  }

      def processUniqueMotif_1Edges(g:GraphFrame, motif:String) : GraphFrame =
  {
			println("graph sizev ", g.vertices.count)
			println("graph size e", g.edges.count)
			val nodeReuse : scala.collection.mutable.ArrayBuffer[Int] =  scala.collection.mutable.ArrayBuffer.fill(3)(0)
			val overlappingMotifs = g.find(motif)
			overlappingMotifs.show(10)
			val selectEdgeArr = Array("e1.src", "e1.dst", "e1.time")
			val selctedMotifEdges = overlappingMotifs.select(selectEdgeArr.head, selectEdgeArr.tail: _*)

			// get unique motif
			val edgeRdd = selctedMotifEdges.rdd
			val uniqueE1 = edgeRdd.map(row => (row.getAs[Int](0), row.getAs[Int](1), row.getAs[Long](2))).distinct
			
				
			val uniqueMotifCount = uniqueE1.count.toInt
			println("All count is ", uniqueE1.count)
			println("unique count is ", uniqueMotifCount)
			//get uniqueMotifCount number of samples for each edge
			val e1 = uniqueE1.take(uniqueMotifCount)

			       /*
			 * construct motif from edges to compute their information content
			 */
			val allmotifs : ListBuffer[(  (Int,Int,Long))] = ListBuffer.empty
			for( i <- 0 to uniqueMotifCount - 1)
			{
				allmotifs += ( ((e1(i)._1,e1(i)._2,e1(i)._3) ))
						
						
				var reusedNodes = Array(0,0);
				//If ith motif's e1 edge's 1st entry (i.e. src) time is less than the edge time 
				// then we have already seen the src. So put 1
			  if(localVAppearanceTime.getOrElse(e1(i)._1, -1L) < e1(i)._3) reusedNodes(0) = 1
				//If ith motif's e1 edge's 2st entry (i.e. dst) time is less than the edge time 
				// then we have already seen the dst. So put 1
			  if(localVAppearanceTime.getOrElse(e1(i)._2, -1L) < e1(i)._3) reusedNodes(1) = 1
			  //7265totl 0 = (1625370904,1306605644,1375426800) : 1375426800:1360310400ArrayBuffer(2272, 3924, 1070)
			  //7360totl 0 = (-1771890193,-81644893,1379660400) : 1379660400:1379660400ArrayBuffer(2296, 3981, 1084)
			  /*
			   * TODO
			   * Above example shows that if two edges with a common vertex are added at the same time to the grpah, 
			   * this code count them twice, and that's why there is more number of vertex generated than the original one
			   * May be track all the vertex already marked as new vertex.
			   */
			  
			  val totalVisibleNodes = reusedNodes(0) + reusedNodes(1)
			  
			  // lets say totalVisibleNodes is 2, i.e. 2 nodes were reused, so increment the counter
			  // of where 2 nodes were reused
			  nodeReuse(totalVisibleNodes) = nodeReuse(totalVisibleNodes) + 1
			  //println(i +"totl " + totalVisibleNodes +" = " + ( ((e1(i)._1,e1(i)._2,e1(i)._3) )) + " : " + localVAppearanceTime.getOrElse(e1(i)._1, -1L) + ":" + localVAppearanceTime.getOrElse(e1(i)._2, -1L) + nodeReuse)
			}
			MInfo += nodeReuse.toList
			
			// Dont need to compute motif structure to update dataframe. Just create a big
			// array of all unique edges and use that
			val uniqeE = e1
			val uniqeEDF = sqlContext.createDataFrame(uniqeE).toDF("src", "dst", "time")
			/*
         * 			dataFrame's except methods returns distinct edges by default. 
         * 			See more detail in processUniqueMotif_3Edges method
         * 
         */
			val newEDF = g.edges.except(uniqeEDF)
			import sqlContext.implicits._
			val newVRDD = newEDF.flatMap(nd => Iterator((nd.getAs[Int](0), nd.getAs[Int](0)),
				(nd.getAs[Int](1), nd.getAs[Int](1)))).distinct.toDF("id", "name")
			import sqlContext.implicits._
			val newGraph = GraphFrame(newVRDD, newEDF)
			return newGraph
		}
     def processUniqueMotif_2Edges3Vertices(g:GraphFrame, motif:String,symmetry:Boolean = false) : GraphFrame =
  {
    	    println("graph sizev ", g.vertices.count)
    	    println("graph size e", g.edges.count)
				val nodeReuse : scala.collection.mutable.ArrayBuffer[Int] =  scala.collection.mutable.ArrayBuffer.fill(4)(0)
				var tmpG = g
					val overlappingMotifs = if (symmetry)
						tmpG.find(motif).filter("a != b").filter("b != c").filter("c != a").filter("e1.time < e2.time")
					else
						tmpG.find(motif).filter("a != b").filter("b != c").filter("c != a")
					overlappingMotifs.show(10)
					val selectEdgeArr = Array("e1.src", "e1.dst", "e1.time", "e2.src", "e2.dst", "e2.time")
					val selctedMotifEdges = overlappingMotifs.select(selectEdgeArr.head, selectEdgeArr.tail: _*)

					// get unique motif
					val edgeRdd = selctedMotifEdges.rdd
					println("total initial motifs size 2 ", edgeRdd.count)
					val uniqueE1 = edgeRdd.map(row => (row.getAs[Int](0), row.getAs[Int](1), row.getAs[Long](2))).distinct
					val uniqueE2 = edgeRdd.map(row => (row.getAs[Int](3), row.getAs[Int](4), row.getAs[Long](5))).distinct

					val e2dash = uniqueE2.subtract(uniqueE1)
					val e2dashBrC = sc.broadcast(e2dash.collect).value

					val newMin = Math.min(uniqueE1.count, e2dash.count).toInt
					println("All count is ", uniqueE1.count, uniqueE2.count)
					val e1 = uniqueE1.take(newMin)
					val e2 = e2dash.take(newMin)
		      val validMotifs = edgeRdd.filter(row => e2dashBrC.contains((row.getAs[Int](3), row.getAs[Int](4), row
			 .getAs[Long](5)))).take(newMin)
					println("valid motif count is ", validMotifs.length)
					println("new min  count is newMin ", newMin)

					/*
					 * construct motif from edges to compute their information content
					 */
					val allmotifs: ListBuffer[((Int, Int, Long), (Int, Int, Long))] = ListBuffer.empty
					for (i <- 0 to validMotifs.length - 1) {

						val minTime = Math.min(validMotifs(i).getAs[Long](3), validMotifs(i).getAs[Long](7))
						var numReusedNodes = 0;
						// this method only covers 2E3V motif which has 2 versions. ab ac , and ba ca 
						// 3 nodes are at (0th, 1st, 4th) and (0th, 1st, 3rd)
						if(validMotifs(i).getAs[Int](0) == validMotifs(i).getAs[Int](3))
						{
							// 3 nodes are at (0th, 1st, 4th) ab ac
							if (localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](0), -1L) < minTime)
							numReusedNodes = numReusedNodes + 1
							if (localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](1), -1L) < minTime)
							numReusedNodes = numReusedNodes + 1
							if (localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](4), -1L) < minTime)
							numReusedNodes = numReusedNodes + 1
						}
						else
						{
							// 3 nodes are at (0th, 1nd, 3rd) ba ca
							if (localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](0), -1L) < minTime)
								numReusedNodes = numReusedNodes + 1
							if (localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](1), -1L) < minTime)
								numReusedNodes = numReusedNodes + 1
							if (localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](3), -1L) < minTime)
								numReusedNodes = numReusedNodes + 1
							}

						// lets say totalVisibleNodes is 2, i.e. 2 nodes were reused, so increment the counter
						// of where 2 nodes were reused
						nodeReuse(numReusedNodes) = nodeReuse(numReusedNodes) + 1
					}
					MInfo += nodeReuse.toList

					// Dont need to compute motif structure to update dataframe. Just create a big
					// array of all unique edges and use that
					val uniqeE = e1 ++ e2
					val uniqeEDF = sqlContext.createDataFrame(uniqeE).toDF("src", "dst", "time")
			/*
         * 			dataFrame's except methods returns distinct edges by default. 
         * 			See more detail in processUniqueMotif_3Edges method
         * 
         */
			val newEDF = g.edges.except(uniqeEDF)
					import sqlContext.implicits._
					val newVRDD = newEDF.flatMap(nd => Iterator((nd.getAs[Int](0), nd.getAs[Int](0)),
						(nd.getAs[Int](1), nd.getAs[Int](1)))).distinct.toDF("id", "name")
					import sqlContext.implicits._
					val newGraph = GraphFrame(newVRDD, newEDF)
					tmpG = newGraph

				
     return tmpG
  }
    
    def processUniqueMotif_2Edges(g:GraphFrame, motif:String,symmetry:Boolean = false) : GraphFrame =
  {
    	    println("graph sizev ", g.vertices.count)
    println("graph size e", g.edges.count)
				val nodeReuse : scala.collection.mutable.ArrayBuffer[Int] =  scala.collection.mutable.ArrayBuffer.fill(3)(0)

      val overlappingMotifs = if(symmetry)
    															g.find(motif).filter("e1.time < e2.time")
    														else
    														  g.find(motif)
    		overlappingMotifs.show(10)
        val selectEdgeArr = Array("e1.src", "e1.dst", "e1.time","e2.src", "e2.dst", "e2.time")
        val selctedMotifEdges = overlappingMotifs.select(selectEdgeArr.head, selectEdgeArr.tail: _*)

        // get unique motif
        val edgeRdd = selctedMotifEdges.rdd
        println("total initial motifs size 2 " , edgeRdd.count)
        val uniqueE1 = edgeRdd.map(row=>(row.getAs[Int](0),row.getAs[Int](1),row.getAs[Long](2))).distinct
        val uniqueE2 = edgeRdd.map(row=>(row.getAs[Int](3),row.getAs[Int](4),row.getAs[Long](5))).distinct
        
        val e2dash = uniqueE2.subtract(uniqueE1)
        val e2dashBrC = sc.broadcast(e2dash.collect).value
        
        val newMin = Math.min(uniqueE1.count, e2dash.count).toInt
        println("All count is ", uniqueE1.count, uniqueE2.count)
        val e1 = uniqueE1.take(newMin)
        val e2 = e2dash.take(newMin)
        val validMotifs = edgeRdd.filter(row
							=>e2dashBrC.contains((row.getAs[Int](3), row.getAs[Int](4), row.getAs[Long](5)))).take(newMin)
		  println("valid motif count is ", validMotifs.length)
		  println("new min  count is newMin ", newMin)
		  
        /*
			 * construct motif from edges to compute their information content
			 */
			val allmotifs : ListBuffer[(  (Int,Int,Long),(Int,Int,Long) )] = ListBuffer.empty
			for( i <- 0 to validMotifs.length - 1)
			{

				val minTime = Math.min(validMotifs(i).getAs[Long](2), validMotifs(i).getAs[Long](5))
				var numReusedNodes = 0;
				if(localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](0), -1L) < minTime)
					numReusedNodes = numReusedNodes +1
				if(localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](1), -1L) < minTime)
					numReusedNodes = numReusedNodes +1
				
			/*	var reusedNodes = Array(0,0);
				//If ith motif's e1 edge's 1st entry (i.e. src) time is less than the edge time 
				// then we have already seen the src. So put 1
			  if(localVAppearanceTime.getOrElse(e1(i)._1, -1L) < e1(i)._3) reusedNodes(0) = 1
				//If ith motif's e1 edge's 2st entry (i.e. dst) time is less than the edge time 
				// then we have already seen the dst. So put 1
			  if(localVAppearanceTime.getOrElse(e1(i)._2, -1L) < e1(i)._3) reusedNodes(1) = 1
			  //If ith motif's e2 edge's 1st entry (i.e. src) time is less than the edge time 
				// then we have already seen the src. So put 1
			  if(localVAppearanceTime.getOrElse(e2(i)._1, -1L) < e2(i)._3) reusedNodes(1) = 1
			  if(localVAppearanceTime.getOrElse(e2(i)._2, -1L) < e2(i)._3) reusedNodes(0) = 1
			  
			  val totalVisibleNodes = reusedNodes(0) + reusedNodes(1)
			*/  
			  // lets say totalVisibleNodes is 2, i.e. 2 nodes were reused, so increment the counter
			  // of where 2 nodes were reused
			  nodeReuse(numReusedNodes) = nodeReuse(numReusedNodes) + 1
			}
			MInfo += nodeReuse.toList
			
        // Dont need to compute motif structure to update dataframe. Just create a big
        // array of all unique edges and use that
        val uniqeE = e1 ++ e2
        val uniqeEDF = sqlContext.createDataFrame(uniqeE).toDF("src", "dst", "time")
        /*
         * 			dataFrame's except methods returns distinct edges by default. 
         * 			See more detail in processUniqueMotif_3Edges method
         * 
         */
        val newEDF = g.edges.except(uniqeEDF)
        import sqlContext.implicits._
        val newVRDD = newEDF.flatMap(nd=>Iterator((nd.getAs[Int](0),nd.getAs[Int](0)),  
            (nd.getAs[Int](1),nd.getAs[Int](1)))).distinct.toDF("id", "name")
        import sqlContext.implicits._
        val newGraph = GraphFrame(newVRDD, newEDF)
        return newGraph
  }
    
  def processUniqueMotif_3Edges(g:GraphFrame, motif:String,symmetry:Boolean = false) : GraphFrame =
  {
			println("graph sizev ", g.vertices.count)
			println("graph size e", g.edges.count)
			val nodeReuse : scala.collection.mutable.ArrayBuffer[Int] =  scala.collection.mutable.ArrayBuffer.fill(4)(0)
			val overlappingMotifs = if (symmetry)
				g.find(motif).filter("e1.time < e2.time").filter("e2.time < e3.time")
			else
				g.find(motif)
			overlappingMotifs.show(10)
			val selectEdgeArr = Array("e1.src", "e1.dst", "e1.time", "e2.src", "e2.dst", "e2.time", "e3.src", "e3.dst", "e3.time")
			val selctedMotifEdges = overlappingMotifs.select(selectEdgeArr.head, selectEdgeArr.tail: _*)

			// get unique motif
			val edgeRdd = selctedMotifEdges.rdd
			println("total initial motifs " , edgeRdd.count)
			val uniqueE1 = edgeRdd.map(row => (row.getAs[Int](0), row.getAs[Int](1), row.getAs[Long](2))).distinct
			val uniqueE2 = edgeRdd.map(row => (row.getAs[Int](3), row.getAs[Int](4), row.getAs[Long](5))).distinct
			val uniqueE3 = edgeRdd.map(row => (row.getAs[Int](6), row.getAs[Int](7), row.getAs[Long](8))).distinct
			
			/*
			 * +-------+-------------------+---------+--------------------+---------+-------------------+
					|      a|                 e1|        b|                  e2|        c|                 e3|
					+-------+-------------------+---------+--------------------+---------+-------------------+
					|[11,11]|[11,156,1363017354]|[156,156]|[156,564,1362170295]|[564,564]|[564,11,1358808018]|
					|[11,11]|[11,156,1363017354]|[156,156]|[156,564,1358803055]|[564,564]|[564,11,1358808018]|
					|[11,11]|[11,156,1363017354]|[156,156]|[156,564,1359737120]|[564,564]|[564,11,1358808018]|
			 */
			/*
			 * find if there are any common edges
			 */
			val e2dash = uniqueE2.subtract(uniqueE1)
			val e2dashBrC = sc.broadcast(e2dash.collect).value
			val e3dashdash = uniqueE3.subtract(uniqueE1).subtract(e2dash)
			val e3dashdashBrC = sc.broadcast(e3dashdash.collect).value
			val newMin = Math.min(uniqueE1.count, Math.min(e2dash.count, e3dashdash.count)).toInt
				//(number of commetn element in e1,e2,,39)

				//for symmetry=false
				/*
				 * (number of commetn element in e1,e2,,787)
						(number of commetn element in e1,e3,,1229)
						(number of commetn element in e2,e3,,1257)
				 */
			println("All count is ", uniqueE1.count, e2dash.count, e3dashdash.count)
			println("unique count is ", newMin)
			
			val validMotifs = edgeRdd.filter(row 
					=> e3dashdashBrC.contains((row.getAs[Int](6), row.getAs[Int](7), row.getAs[Long](8)))).filter(row
							=>e2dashBrC.contains((row.getAs[Int](3), row.getAs[Int](4), row.getAs[Long](5)))).take(newMin)
							/*
							 * without take min, it has multiple options of validMotifs
							 * (363,238,1358191071,238,2822,1358209322,2822,363,1359754984)
									(363,238,1358191071,238,2822,1358209322,2822,363,1358791030)
									(363,238,1358192503,238,2822,1358209322,2822,363,1359754984)
									(363,238,1358192503,238,2822,1358209322,2822,363,1358791030)
									(363,238,1358193658,238,2822,1358209322,2822,363,1359754984)
									(363,238,1358193658,238,2822,1358209322,2822,363,1358791030)
									(363,238,1358194778,238,2822,1358209322,2822,363,1359754984)
									(363,238,1358194778,238,2822,1358209322,2822,363,1358791030)
							 */
		  println("valid motif count is ", validMotifs.length)
		  /*validMotifs.foreach(row=>println((row.getAs[Int](0), row.getAs[Int](1), row.getAs[Long](2),
		  		row.getAs[Int](3), row.getAs[Int](4), row.getAs[Long](5),
		  		row.getAs[Int](6), row.getAs[Int](7), row.getAs[Long](8))))
			*///get uniqueMotifCount number of samples for each edge
			val e1 = uniqueE1.take(newMin)
			val e2 = e2dash.take(newMin)
			val e3 = e3dashdash.take(newMin)
			/*
			 * construct motif from edges to compute their information content
			 */
			for( i <- 0 to validMotifs.length - 1)
			{
//				allmotifs += ( ((e1(i)._1,e1(i)._2,e1(i)._3),
//						(e2(i)._1,e2(i)._2,e2(i)._3),
//						(e3(i)._1,e3(i)._2,e3(i)._3)) )
					val minTime = Math.min(validMotifs(i).getAs[Long](2),
							Math.min(validMotifs(i).getAs[Long](5), validMotifs(i).getAs[Long](8)))
				var numReusedNodes = 0;
				if(localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](0), -1L) < minTime)
					numReusedNodes = numReusedNodes +1
				if(localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](1), -1L) < minTime)
					numReusedNodes = numReusedNodes +1
  			if(localVAppearanceTime.getOrElse(validMotifs(i).getAs[Int](4), -1L) < minTime)
					numReusedNodes = numReusedNodes +1
	
	/*
				var reusedNodes = Array(0,0,0);
				if(symmetry == true)
				{
					//If ith motif's e1 edge's 1st entry (i.e. src) time is less than the edge time 
					// then we have already seen the src. So put 1
					if (localVAppearanceTime.getOrElse(e1(i)._1, -1L) < e1(i)._3) reusedNodes(0) = 1
					//If ith motif's e1 edge's 2st entry (i.e. dst) time is less than the edge time 
					// then we have already seen the dst. So put 1
					if (localVAppearanceTime.getOrElse(e1(i)._2, -1L) < e1(i)._3) reusedNodes(1) = 1
					//If ith motif's e2 edge's 1st entry (i.e. src) time is less than the edge time 
					// then we have already seen the src. So put 1
					if (localVAppearanceTime.getOrElse(e2(i)._1, -1L) < e2(i)._3) reusedNodes(1) = 1
					if (localVAppearanceTime.getOrElse(e2(i)._2, -1L) < e2(i)._3) reusedNodes(2) = 1
					if (localVAppearanceTime.getOrElse(e3(i)._1, -1L) < e3(i)._3) reusedNodes(2) = 1
					if (localVAppearanceTime.getOrElse(e3(i)._2, -1L) < e3(i)._3) reusedNodes(0) = 1

				}
				else
				{
					//If ith motif's e1 edge's 1st entry (i.e. src) time is less than the edge time 
					// then we have already seen the src. So put 1
					if (localVAppearanceTime.getOrElse(e1(i)._1, -1L) < e1(i)._3) reusedNodes(0) = 1
					//If ith motif's e1 edge's 2st entry (i.e. dst) time is less than the edge time 
					// then we have already seen the dst. So put 1
					if (localVAppearanceTime.getOrElse(e1(i)._2, -1L) < e1(i)._3) reusedNodes(1) = 1
					//If ith motif's e2 edge's 1st entry (i.e. src) time is less than the edge time 
					// then we have already seen the src. So put 1
					if (localVAppearanceTime.getOrElse(e2(i)._1, -1L) < e2(i)._3) reusedNodes(1) = 1
					if (localVAppearanceTime.getOrElse(e2(i)._2, -1L) < e2(i)._3) reusedNodes(2) = 1
					//e3 is from a->c not c->a like symmetry case
					if (localVAppearanceTime.getOrElse(e3(i)._1, -1L) < e3(i)._3) reusedNodes(0) = 1
					if (localVAppearanceTime.getOrElse(e3(i)._2, -1L) < e3(i)._3) reusedNodes(2) = 1
				}
				val totalVisibleNodes = reusedNodes(0) + reusedNodes(1) + reusedNodes(2)
*/			  
					
			  
			  // lets say totalVisibleNodes is 2, i.e. 2 nodes were reused, so increment the counter
			  // of where 2 nodes were reused
			  nodeReuse(numReusedNodes) = nodeReuse(numReusedNodes) + 1
			}
			MInfo += nodeReuse.toList
			
			// Dont need to compute motif structure to update dataframe. Just create a big
			// array of all unique edges and use that
			val uniqeE = e1 ++ e2 ++ e3
			val uniqeEDF = sqlContext.createDataFrame(uniqeE).toDF("src", "dst", "time")
			/*
       * 			dataFrame's except methods returns distinct edges by default. 
       *      I dont see the documentation saying this. I have fixed the graph reader code and do a "distinct" while
       *      creating the base RDD
       * 
       *      println("old edge count " , g.edges.count)
			        println("UNIQUEW old edge count " , g.edges.distinct.count)
			        val newEDF = g.edges.except(uniqeEDF)
			        println("new edge count after except ", newEDF.count)
			        
							(old edge count ,502)
							(UNIQUEW old edge count ,454)
							(new count after except  ,451)
       * 
       * 
       */
			val newEDF = g.edges.except(uniqeEDF)
			import sqlContext.implicits._
			val newVRDD = newEDF.flatMap(nd => Iterator((nd.getAs[Int](0), nd.getAs[Int](0)),
				(nd.getAs[Int](1), nd.getAs[Int](1)))).distinct.toDF("id", "name")
			import sqlContext.implicits._
			val newGraph = GraphFrame(newVRDD, newEDF)
			return newGraph
		}
  
  def processMotif(g:GraphFrame,motif:String)
  {
    val motifs = g.find(motif).filter("a != b").filter("a != c").filter("c != b").filter("e1.time < e2.time").filter("e1.time < e3.time").filter("e2.time < e3.time").filter("e3.time - e1.time < 5000")
    val sortedMotif = motifs.sort("e1.time")
    val selectArr = Array("a", "b", "c")
    val selctedNodes = sortedMotif.select(selectArr.head, selectArr.tail: _*)

    /*
     * Get time offiset of edge2 and edge3
     */
    val selectEdgeArr = Array("e1.time", "e2.time", "e3.time")
    val selctedEdges = sortedMotif.select(selectEdgeArr.head, selectEdgeArr.tail: _*)
    val edgeOffset = selctedEdges.rdd.map(row =>  {
      val t2 = row.get(1).toString.toInt 
      val t1 = row.get(0).toString.toInt
      val t3 = row.get(2).toString.toInt 
      (t2-t1,t3-t1)
      
    } )
    val avgE2Offset = edgeOffset.map(t=>t._1).sum/edgeOffset.count
    val avgE3Offset = edgeOffset.map(t=>t._2).sum/edgeOffset.count
    OffsetInfo += ((avgE2Offset.toInt,avgE3Offset.toInt))
    
    val visibleNodes = scala.collection.mutable.Set[Int]()
    val nodeReuse : scala.collection.mutable.ArrayBuffer[Int] =  scala.collection.mutable.ArrayBuffer.fill(4)(0)
    selctedNodes.collect.foreach(row => {
    if(visibleNodes.contains(row.getAs[Int](0)) && visibleNodes.contains(row.getAs[Int](1)) && visibleNodes.contains(row.getAs[Int](2)))
      {
      	nodeReuse(3) = nodeReuse(3) +1
      }
      else 
      	if(visibleNodes.contains(row.getAs[Int](0)) && visibleNodes.contains(row.getAs[Int](1)))
        {
        	nodeReuse(2) = nodeReuse(2) + 1
        }
        else if(visibleNodes.contains(row.getAs[Int](0)))
          {
          	nodeReuse(1) = nodeReuse(1) + 1
          }
        else if(visibleNodes.contains(row.getAs[Int](1)))
          {
          	nodeReuse(1) = nodeReuse(1) + 1
          }
          else
		{
            nodeReuse(0) = nodeReuse(0) + 1
		}

      visibleNodes += (row.getAs[Int](0))
      visibleNodes += (row.getAs[Int](1))
      visibleNodes += (row.getAs[Int](2))
    })
    
 		MInfo += nodeReuse.toList
    println(nodeReuse.toList.toString)
    
  }
  println("M info createe" , MInfo.length)
}
/*
 * val allMotifs = Array("(a)-[e1]->(b); (c)-[e2]->(b); (a)-[e3]->(b)",
        "(a)-[e1]->(b); (c)-[e2]->(b); (b)-[e3]->(a)",
        "(a)-[e1]->(b); (c)-[e2]->(b); (a)-[e3]->(c)",
        "(a)-[e1]->(b); (c)-[e2]->(b); (c)-[e3]->(a)",
        "(a)-[e1]->(b); (c)-[e2]->(b); (b)-[e3]->(c)",
        "(a)-[e1]->(b); (c)-[e2]->(b); (c)-[e3]->(b)",
        "(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(b)",
        "(a)-[e1]->(b); (b)-[e2]->(c); (b)-[e3]->(a)",
        "(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(c)",
        "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)",
        "(a)-[e1]->(b); (b)-[e2]->(c); (b)-[e3]->(c)",
        "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(b)",
        "(a)-[e1]->(b); (c)-[e2]->(a); (a)-[e3]->(b)",
        "(a)-[e1]->(b); (c)-[e2]->(a); (b)-[e3]->(a)",
        "(a)-[e1]->(b); (c)-[e2]->(a); (a)-[e3]->(c)",
        "(a)-[e1]->(b); (c)-[e2]->(a); (c)-[e3]->(a)",
        "(a)-[e1]->(b); (c)-[e2]->(a); (b)-[e3]->(c)",
        "(a)-[e1]->(b); (c)-[e2]->(a); (c)-[e3]->(b)",
        "(a)-[e1]->(b); (a)-[e2]->(c); (a)-[e3]->(b)",
        "(a)-[e1]->(b); (a)-[e2]->(c); (b)-[e3]->(a)",
        "(a)-[e1]->(b); (a)-[e2]->(c); (a)-[e3]->(c)",
        "(a)-[e1]->(b); (a)-[e2]->(c); (c)-[e3]->(a)",
        "(a)-[e1]->(b); (a)-[e2]->(c); (b)-[e3]->(c)",
        "(a)-[e1]->(b); (a)-[e2]->(c); (c)-[e3]->(b)",
        //"(a)-[e1]->(b); (b)-[e2]->(a); (a)-[e3]->(b)",
        //"(a)-[e1]->(b); (b)-[e2]->(a); (b)-[e3]->(a)",
        "(a)-[e1]->(b); (b)-[e2]->(a); (a)-[e3]->(c)",
        "(a)-[e1]->(b); (b)-[e2]->(a); (c)-[e3]->(a)",
        "(a)-[e1]->(b); (b)-[e2]->(a); (b)-[e3]->(c)",
        "(a)-[e1]->(b); (b)-[e2]->(a); (c)-[e3]->(b)",
        //"(a)-[e1]->(b); (a)-[e2]->(b); (a)-[e3]->(b)",
        //"(a)-[e1]->(b); (a)-[e2]->(b); (b)-[e3]->(a)",
        "(a)-[e1]->(b); (a)-[e2]->(b); (a)-[e3]->(c)",
        "(a)-[e1]->(b); (a)-[e2]->(b); (c)-[e3]->(a)",
        "(a)-[e1]->(b); (a)-[e2]->(b); (b)-[e3]->(c)",
        "(a)-[e1]->(b); (a)-[e2]->(b); (c)-[e3]->(b)")
        
        
        duplicate edges in input data 500
        ([0,1,1375601524],2)
([16,1,1368342714],2)
([0,1,1368343134],2)
([21,1,1367392664],2)
([21,1,1370157492],2)
([0,1,1368947938],2)
([2,1,1368342500],2)
([15,1,1375342824],2)
([2,1,1374476912],2)
([21,1,1367738272],2)
([16,1,1375600390],2)
([15,1,1367393064],2)
([16,1,1375341280],2)
([15,1,1372664118],2)
([2,1,1373267350],2)
([0,1,1370071164],2)
([0,1,1374478282],2)
([15,1,1373873828],2)
([0,1,1375342470],2)
([15,1,1372664240],2)
([2,1,1368947302],2)
([16,1,1368947520],2)
([2,1,1367392116],2)
([3,46,1355711972],2)
([15,1,1370762862],2)
([16,1,1367737938],2)
([15,1,1373268418],2)
([16,1,1374477164],2)
([0,1,1373873484],2)
([2,1,1375341032],2)
([0,1,1372663522],2)
([0,1,1367392756],2)
([2,1,1370156948],2)
([16,1,1372662920],2)
([15,1,1368948248],2)
([16,1,1367392334],2)
([21,1,1368343046],2)
([2,1,1367737722],2)
([15,1,1375601872],2)
([2,1,1375600140],2)
([0,1,1370157582],2)
([16,1,1373267598],2)
([21,1,1370071074],2)
([2,1,1373872110],2)
([0,1,1367738362],2)
([2,1,1370070520],2)
([21,1,1368947848],2)
([16,1,1372662798],2)
 */
/*
 * Type prefix_annotation without space: 
testingMissinbV
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
18/01/30 23:59:11 INFO SparkContext: Running Spark version 2.1.0
18/01/30 23:59:11 WARN SparkContext: Support for Scala 2.10 is deprecated as of Spark 2.1.0
18/01/30 23:59:13 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/01/30 23:59:13 INFO SecurityManager: Changing view acls to: puro755
18/01/30 23:59:13 INFO SecurityManager: Changing modify acls to: puro755
18/01/30 23:59:13 INFO SecurityManager: Changing view acls groups to: 
18/01/30 23:59:13 INFO SecurityManager: Changing modify acls groups to: 
18/01/30 23:59:13 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(puro755); groups with view permissions: Set(); users  with modify permissions: Set(puro755); groups with modify permissions: Set()
18/01/30 23:59:13 INFO Utils: Successfully started service 'sparkDriver' on port 53070.
18/01/30 23:59:13 INFO SparkEnv: Registering MapOutputTracker
18/01/30 23:59:13 INFO SparkEnv: Registering BlockManagerMaster
18/01/30 23:59:13 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
18/01/30 23:59:13 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
18/01/30 23:59:13 INFO DiskBlockManager: Created local directory at /private/var/folders/0r/vnc8s2_j1gq3_v8dlbvvv4gm33fnj3/T/blockmgr-393b0751-dcb6-4188-a3a4-e2439de6727c
18/01/30 23:59:13 INFO MemoryStore: MemoryStore started with capacity 2004.6 MB
18/01/30 23:59:13 INFO SparkEnv: Registering OutputCommitCoordinator
18/01/30 23:59:13 INFO Utils: Successfully started service 'SparkUI' on port 4040.
18/01/30 23:59:13 INFO SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.1.8:4040
18/01/30 23:59:14 INFO Executor: Starting executor ID driver on host localhost
18/01/30 23:59:14 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 53071.
18/01/30 23:59:14 INFO NettyBlockTransferService: Server created on 192.168.1.8:53071
18/01/30 23:59:14 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
18/01/30 23:59:14 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.1.8, 53071, None)
18/01/30 23:59:14 INFO BlockManagerMasterEndpoint: Registering block manager 192.168.1.8:53071 with 2004.6 MB RAM, BlockManagerId(driver, 192.168.1.8, 53071, None)
18/01/30 23:59:14 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.1.8, 53071, None)
18/01/30 23:59:14 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, 192.168.1.8, 53071, None)
(M info createe,0)
(spark.master,local)
(spark.serializer,org.apache.spark.serializer.KryoSerializer)
(spark.app.name,NOUS Graph Pattern Miner)
(spark.driver.host,192.168.1.8)
(spark.kryo.classesToRegister,)
(spark.app.id,local-1517385554025)
(spark.executor.id,driver)
(spark.shuffle.blockTransferService,nio)
(spark.rdd.compress,true)
(spark.driver.port,53070)
(nodeQuadruples siEZ is ,19329)
(min time,1351754432)
(max time,1379995220)
(duration,28240788)
(number of vertex ,3504)
(number of edges ,19329)
(graph sizev ,3504)
(graph size e,19329)
+-----------+--------------------+-----------+--------------------+---------+--------------------+
|          a|                  e1|          b|                  e2|        c|                  e3|
+-----------+--------------------+-----------+--------------------+---------+--------------------+
|[1278,1278]|[1278,650,1360002...|  [650,650]|[650,902,1360006256]|[902,902]|[902,1278,1360014...|
|[1278,1278]|[1278,650,1360002...|  [650,650]|[650,902,1360006256]|[902,902]|[902,1278,1374505...|
|  [134,134]|[134,1612,1358207...|[1612,1612]|[1612,588,1360014...|[588,588]|[588,134,1363291734]|
|  [134,134]|[134,1612,1358207...|[1612,1612]|[1612,588,1360012...|[588,588]|[588,134,1363291734]|
|  [134,134]|[134,1612,1358208...|[1612,1612]|[1612,588,1360014...|[588,588]|[588,134,1363291734]|
|  [134,134]|[134,1612,1358208...|[1612,1612]|[1612,588,1360012...|[588,588]|[588,134,1363291734]|
|  [134,134]|[134,1612,1358189...|[1612,1612]|[1612,588,1360014...|[588,588]|[588,134,1363291734]|
|  [134,134]|[134,1612,1358189...|[1612,1612]|[1612,588,1360012...|[588,588]|[588,134,1363291734]|
|[2327,2327]|[2327,530,1358194...|  [530,530]|  [530,6,1358781084]|    [6,6]| [6,2327,1358790538]|
|[2327,2327]|[2327,530,1358194...|  [530,530]|  [530,6,1358787092]|    [6,6]| [6,2327,1358790538]|
+-----------+--------------------+-----------+--------------------+---------+--------------------+
only showing top 10 rows

(total initial motifs ,3086)
(All count is ,547,334,299)
(unique count is ,299)
(valid motif count is ,299)
(graph sizev ,3502)
(graph size e,18432)
+-----------+--------------------+-----------+--------------------+-----------+--------------------+
|          a|                  e1|          b|                  e2|          c|                  e3|
+-----------+--------------------+-----------+--------------------+-----------+--------------------+
|    [26,26]|[26,2403,1364243268]|[2403,2403]|[2403,2403,135878...|[2403,2403]|[26,2403,1364243268]|
|    [26,26]|[26,2403,1364243268]|[2403,2403]|[2403,2403,135878...|[2403,2403]|[26,2403,1364243268]|
|[2155,2155]|[2155,2182,135820...|[2182,2182]|[2182,1970,136000...|[1970,1970]|[2155,1970,135878...|
|[2155,2155]|[2155,2182,135820...|[2182,2182]|[2182,1970,136000...|[1970,1970]|[2155,1970,135878...|
|[2155,2155]|[2155,2182,135714...|[2182,2182]|[2182,1970,136000...|[1970,1970]|[2155,1970,135878...|
|[2155,2155]|[2155,2182,135714...|[2182,2182]|[2182,1970,136000...|[1970,1970]|[2155,1970,135878...|
|[2494,2494]|[2494,427,1359742...|  [427,427]| [427,63,1359730112]|    [63,63]|[2494,63,1358789064]|
|[2494,2494]|[2494,427,1359742...|  [427,427]| [427,63,1359730112]|    [63,63]|[2494,63,1358789062]|
|[2494,2494]|[2494,427,1359742...|  [427,427]| [427,63,1357168734]|    [63,63]|[2494,63,1358789064]|
|[2494,2494]|[2494,427,1359742...|  [427,427]| [427,63,1357168734]|    [63,63]|[2494,63,1358789062]|
+-----------+--------------------+-----------+--------------------+-----------+--------------------+
only showing top 10 rows

(total initial motifs ,16972)
(All count is ,2014,1272,515)
(unique count is ,515)
(valid motif count is ,515)
(graph sizev ,3497)
(graph size e,16887)
+---------+--------------------+-----------+--------------------+
|        a|                  e1|          b|                  e2|
+---------+--------------------+-----------+--------------------+
|[849,849]|[849,1383,1358202...|[1383,1383]|[1383,849,1358203...|
|[849,849]|[849,1383,1358202...|[1383,1383]|[1383,849,1358203...|
|[297,297]|[297,1626,1358187...|[1626,1626]|[1626,297,1358189...|
|[846,846]|[846,2145,1358204...|[2145,2145]|[2145,846,1358204...|
|  [50,50]|  [50,51,1361226925]|    [51,51]|  [51,50,1361229706]|
|  [50,50]|  [50,51,1361226925]|    [51,51]|  [51,50,1361232172]|
|  [50,50]|  [50,51,1361214132]|    [51,51]|  [51,50,1361229706]|
|  [50,50]|  [50,51,1361214132]|    [51,51]|  [51,50,1361232172]|
|  [50,50]|  [50,51,1361149585]|    [51,51]|  [51,50,1361229706]|
|  [50,50]|  [50,51,1361149585]|    [51,51]|  [51,50,1361232172]|
+---------+--------------------+-----------+--------------------+
only showing top 10 rows

(total initial motifs size 2 ,10635)
(All count is ,3062,3096)
(valid motif count is ,1706)
(new min  count is newMin ,1706)
(graph sizev ,3465)
(graph size e,13475)
+-----------+--------------------+-----------+
|          a|                  e1|          b|
+-----------+--------------------+-----------+
|    [47,47]| [47,833,1354554806]|  [833,833]|
|[1934,1934]|[1934,833,1359995...|  [833,833]|
|[1934,1934]|[1934,833,1359731...|  [833,833]|
|[1934,1934]|[1934,833,1367423...|  [833,833]|
|[1934,1934]|[1934,833,1359758...|  [833,833]|
|  [391,391]|[391,833,1354570358]|  [833,833]|
|[1094,1094]|[1094,833,1358804...|  [833,833]|
|[1935,1935]|[1935,833,1359999...|  [833,833]|
|  [321,321]|[321,1088,1358802...|[1088,1088]|
|  [748,748]|[748,1088,1355160...|[1088,1088]|
+-----------+--------------------+-----------+
only showing top 10 rows

(All count is ,13475)
(unique count is ,13475)
List(List(5, 5, 16, 273), List(2, 10, 130, 373), List(36, 141, 1529), List(454, 2130, 10891))
(number of edges in last graph,0)
(number of vertex in last graph,0)
 * 
 */
