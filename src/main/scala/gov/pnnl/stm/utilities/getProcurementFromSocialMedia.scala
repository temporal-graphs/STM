/**
 *
 * @author puro755
 * @dFeb 28, 2018
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.spark.rdd.RDD
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * @author puro755
 *
 */
/*
 * NOTE : NOW THIS FILE IS USED TO ADD PROCURMETN EDGES TO A COMPOSITE GRAPH OF COMMUNICATION AND SOCIAL EDGES
 * COMM IS TYPE 0, SOCIAL IS TYPE 1. 
 * EVERY EDGE OF TYPE 1 IS EXPANDED TO 3 EDGES:
 * 	-- ORIGINAL EDGE
 *  -- SRC 2 HSCODE(SELLS)
 *  -- DST 3 HSCODE(BUYS)
 */
object getProcurementFromSocialMedia {

	def main(args: Array[String]): Unit = {
		val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
		val sc = SparkContextInitializer.getSparkContext(sparkConf)
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/twitterMentionRegenGSG10K.csv"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/STM_StructureTemporalModeling/phone1M-s1STTLined"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/phoneSocialProc2MNodesMultiTypeWorking2PushStar"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/phoneSocialProc2MNodesMultiTypeWorking2StarPushWithProcEdgesSocialLocation.csv"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1.March16.PhoneOnly"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1March18NolabelNoDQNoBrFinalGraphPhoneOnly"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1March18NolabelNoDQNoBrFinalGraph"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1March19PatFinalG1"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDenseSyllogism-s1March19G2Pat"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDensePuma-s1March19G12Pat"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDenseHPrayaas-s1March19G13Pat"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDenseH11-s1March21G11Pat"
				//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/Data/CollegeMsg-etype.txt.small"
		//val nodeFile = "/Users/puro755/OneDrive - " +
			//							"PNNL/PhD/GraphGeneration/graph-stream-generator/June2018V4Release" +
				//						 "/compositePhoneEmail2MNodesDenseJune2018Puma-s1May5.clean"
		val nodeFile = "/Volumes/Darpa-SDGG/pmackey/data/10k/phoneEmail_10k_12.csv"
		val sep=","

		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/June2018V4Release/compositePhoneEmail2MNodesDenseJune2018Syllogism1.0-s1May5.clean"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/June2018V4Release/compositePhoneEmail2MNodesDenseJune2018H13-s1May7.clean"
	/*
	 * get hs code class distribution
	 */
		
				val hsClsCodeFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/AllHSCodePIERS2013IMP.txtDetailedOP/allParts.csv"
		val HSClsCodeProbs: RDD[(Int, Int, Array[Int])] =
			sc.textFile(hsClsCodeFile).map { line =>
				{
					//9403=548623,940360=102118,940390=24208,940310=493,940380=3902,9403=388455,940350=7742,940320=16524,940330=772,940340=1745,940370=2664
					val lineArr = line.split(",")
					//Making a 100 size array for all items in this class
					val HSclass = lineArr(0)
					val allItesm = lineArr.slice(1, lineArr.length)
					val Sum = HSclass.split("=")(1).toInt // HSClass is 9403=548623
					val newAllItems = allItesm.map(item => ((item.split("=")(0).toInt, scala.math.ceil(100 * item.split("=")(1).toDouble / Sum))))
					var arrBuffer: scala.collection.mutable.ArrayBuffer[Int] = new scala.collection.mutable.ArrayBuffer()
					newAllItems.foreach(item => {
						for (i <- 0 to item._2.toInt)
							arrBuffer += item._1
					})
					// At the end of it, arrBuffer lenght may be more than 100
					(HSclass.split("=")(0).toInt, Sum, arrBuffer.toArray)
				}
			}//.sortBy(_._2,false).collect
      val total = HSClsCodeProbs.map(f=>f._2).sum
      // used 1000 to give more diverse probabilites. otherwise most of the things are 3,2 instead 29,31,27,21,20...etc
      val HSClsCodeNormProbs: Array[(Int, Int, Array[Int])] = HSClsCodeProbs.map(f=>(f._1,scala.math.ceil(1000*f._2/total).toInt,f._3)).collect
      var arrBuffer2 : scala.collection.mutable.ArrayBuffer[Int] = new scala.collection.mutable.ArrayBuffer()
      HSClsCodeNormProbs.map(cls=>{
      	for(i<-0 to cls._2)
      		arrBuffer2 += cls._1
      })
      val clsArray : Array[Int]= arrBuffer2.toArray
      val HSClsCodeMap = HSClsCodeNormProbs.map(hs=>(hs._1,hs._3)).toMap
      
      /*
		 * take top 2% high degree nodes and then 6% neighbourhood of it to create procurement edges
		 */
      
      // edge is <src eType dst time Wt>  Wt is 1 on March 12, 2018
			val compositeGraphEdges: RDD[(Int,Int,Int, Long,Int)] =
      sc.textFile(nodeFile).filter(line=> !line.startsWith("Source")).map { line =>
      	val lineArr = line.split(sep)
      	(lineArr(0).toInt,lineArr(1).toInt,lineArr(2).toInt,lineArr(3).toLong,1)
      }
      
      val phoneGraph = compositeGraphEdges.filter(e=>e._2 ==0)
      val phoneNodes = phoneGraph.flatMap(e=>Iterable(e._1, e._3)).distinct.collect
      val rn = Random
      val clsArrsize = clsArray.length
      
      
      // map[phone node--> hsclass)
      val gVerticesHSClassMap = phoneNodes.map(n=>{
      	(n,clsArray(rn.nextInt(clsArrsize)))
      }).toMap
      
		val degD = compositeGraphEdges.filter(e=>e._2==0).flatMap(e
				=>Iterator((e._1,1),(e._3,1))).reduceByKey((cnt1,cnt2) => cnt1+cnt2)// get deg of src and dst
		val topNodes = degD.filter(v=>v._2 > 10000)
		val cnt = compositeGraphEdges.count
    val hdcodeIDOffset = cnt
		print("hdcodeIDOffse" , hdcodeIDOffset)
		//val rn = Random
		// Forcing a collect here as for some reason random selection in distrubuted fasion not working
		val procurementCompositeEdges = compositeGraphEdges.flatMap(sEdge => {
			val rn = new scala.util.Random()
			//randomly pick an hscode, it is already expanded according to original probs.
			// get 2 edges for each social edge. src->hscode , dst->hscode
			if((sEdge._2 == 0)) //&& (top2PercentNodes.contains(sEdge._1) || top2PercentNodes.contains(sEdge._3)))
				// is comm AND part of top 2 percent edges edge which needs to be expanded
			{
				
				{ // if not part of top nodes then with 6% prob
					var	ran = rn.nextInt(100)
					if(ran <= 6)
					{
						/*
						 * First get the HSCode Class
						 */
						
						val scrHSClass=gVerticesHSClassMap.getOrElse(sEdge._1, 9403)
						//9403 is the most used cat 
						
						val srcHScodeItems : Array[Int] = HSClsCodeMap.getOrElse(scrHSClass, Array(940360,940390,940310,940380,9403,940350,940320,940330,940340,940370))
						val totalItmes = srcHScodeItems.length
						val selectedHSCode = srcHScodeItems(rn.nextInt(totalItmes))
						//println("selected hs code is ", selectedHSCode)
					val hsCode = selectedHSCode + hdcodeIDOffset.toInt
							Iterable((sEdge._1, sEdge._2, sEdge._3, sEdge._4, sEdge._5),
						(sEdge._1, 2, hsCode,  sEdge._4, sEdge._5),
					(sEdge._3, 3, hsCode,  sEdge._4, sEdge._5))
					}else
						Iterable((sEdge._1, sEdge._2, sEdge._3, sEdge._4, sEdge._5))
				}
			}else
			{
				Iterable((sEdge._1, sEdge._2, sEdge._3, sEdge._4, sEdge._5))
			}
			
		}).distinct
		//println("procurementCompositeEdges size " , procurementCompositeEdges.length)
		//val outf = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1March18NolabelNoDQNoBrFinalGraphWithProcEdges"))
		//procurementCompositeEdges.foreach(p=>outf.println(p._1 + " " + p._2 + " " + p._3 + " " + p._4 + " " + p._5))
		//outf.flush()
		
		val socialNodes = compositeGraphEdges.filter(e=>e._2 ==1).flatMap(e=>Iterable(e._1, e._3)).distinct
		

		val socialNodeLocationMap = socialNodes.map(n=>{
			val rn = new scala.util.Random()
			
			/*	 * 40%: Asia Pacific
				 * 19: North Amrica
				 * 14: West Europe
				 * 11: Latin America
				 * 8: Middle East & Africa
				 * 8 : Central and Eastern Europe
				 */
				var ran = rn.nextInt(100)
				val Region = if(ran<=40) 1
				else if(ran<59) 2
				else if(ran<73) 3
				else if(ran<84) 4
				else if(ran<92) 5 //bug : earlier it was 82
				else 6
				
				if(Region != 1)
							(n,Region)
				else
					(-1,-1) //just reduce the size of map becuase we are using value 1 as the default region anyway
				
		}).distinct.collect.toMap
		
		val compositeGraphEdgesSocialLocationEdges = procurementCompositeEdges.map(sEdge => {
			if(sEdge._2 == 1) // is social edge which needs to be painted
			{
				

				
				(sEdge._1, sEdge._2, sEdge._3, sEdge._4, " ",socialNodeLocationMap.getOrElse(sEdge._1, 1), socialNodeLocationMap.getOrElse(sEdge._3, 1))
			}else
			{
				(sEdge._1, sEdge._2, sEdge._3, sEdge._4, " ")
			}
			
		})
		println(compositeGraphEdgesSocialLocationEdges.count)
		//compositeGraphEdgesSocialLocationEdges.saveAsTextFile("/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1March18NolabelNoDQNoBrFinalGraphProcurementSocialLocation")
		//compositeGraphEdgesSocialLocationEdges.saveAsTextFile("/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1March19PatFinalG1ProcurementSocialLocation")
		//compositeGraphEdgesSocialLocationEdges.saveAsTextFile("/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDensePuma-s1March19G12PatProcurementSocialLocation")
		//compositeGraphEdgesSocialLocationEdges.saveAsTextFile(nodeFile.replaceAll
		//("compositePhoneEmail2MNodesDenseJune2018", "C1C2C3_V4").replaceAll("-s1", "-").replaceAll(".clean", ""))
		compositeGraphEdgesSocialLocationEdges.saveAsTextFile(nodeFile.replaceAll("compositePhoneEmail2MNodesDenseJune2018",
			"C1C2C3_V4").replaceAll("-s1", "-").replaceAll(".clean", "").replaceAll(".csv","") +"_proc.csv")
	}

}