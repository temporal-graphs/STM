/**
 *
 * @author puro755
 * @dMar 12, 2018
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import gov.pnnl.builders.SparkContextInitializer

/**
 * @author puro755
 *
 */
object paintRegionDistToSocialEdges {

	def main(args: Array[String]): Unit = {
		val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
		val sc = SparkContextInitializer.getSparkContext(sparkConf)
		
				//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/phoneSocialProc2MNodesMultiTypeWorking2WithProcEdges"
				//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/phoneSocialProc2MNodesMultiTypeWorking2PushStar"
				val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/phoneSocialProc2MNodesMultiTypeWorking2StarPushWithProcEdges"
			// edge is <src eType dst time Wt>  Wt is 1 on March 12, 2018
			val compositeGraphEdges: RDD[(Int,Int,Int, Long,Int)] =
      sc.textFile(nodeFile).map { line =>
      	val lineArr = line.split(" ")
      	(lineArr(0).toInt,lineArr(1).toInt,lineArr(2).toInt,lineArr(3).toLong,lineArr(4).toInt)
      	
      }

		//val socialNodes = compositeGraphEdges.filter(e=>e._2 ==1).flatMap(e=>Iterable(e._1, e._3)).distinct
		

/*		val socialNodeLocationMap = socialNodes.map(n=>{
			val rn = new scala.util.Random()
			
				 * 40%: Asia Pacific
				 * 19: North Amrica
				 * 14: West Europe
				 * 11: Latin America
				 * 8: Middle East & Africa
				 * 8 : Central and Eastern Europe
				 
				var ran = rn.nextInt(100)
				val Region = if(ran<=40) 1
				else if(ran<59) 2
				else if(ran<73) 3
				else if(ran<84) 4
				else if(ran<82) 5
				else 6
				(n,Region)
				
		}).toMap*/
		val compositeGraphEdgesSocialLocationEdges = compositeGraphEdges.map(sEdge => {
			val rn = new scala.util.Random()
			if(sEdge._2 == 1) // is social edge which needs to be painted
			{
				
			/*
				 * 40%: Asia Pacific
				 * 19: North Amrica
				 * 14: West Europe
				 * 11: Latin America
				 * 8: Middle East & Africa
				 * 8 : Central and Eastern Europe
				 */
				var ran = rn.nextInt(100)
				val srcRegion = if(ran<=40) 1
				else if(ran<59) 2
				else if(ran<73) 3
				else if(ran<84) 4
				else if(ran<82) 5
				else 6

				ran = rn.nextInt(100)
				val dstRegion = if(ran<=40) 1
				else if(ran<59) 2
				else if(ran<73) 3
				else if(ran<84) 4
				else if(ran<82) 5
				else 6
				
				(sEdge._1, sEdge._2, sEdge._3, sEdge._4, " ",srcRegion, dstRegion)
			}else
			{
				(sEdge._1, sEdge._2, sEdge._3, sEdge._4, " ")
			}
			
		})
		val outf = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/phoneSocialProc2MNodesMultiTypeWorking2StarPushWithProcEdgesSocialLocation.csv"))
		compositeGraphEdgesSocialLocationEdges.foreach(p=>outf.println(p.productIterator.mkString(" ")))
		outf.flush()

	}

}