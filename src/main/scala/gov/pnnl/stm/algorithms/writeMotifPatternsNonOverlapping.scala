/**
 *
 * @author puro755
 * @dJan 2, 2018
 * @Mining
 */
package gov.pnnl.stm.algorithms
import java.io.{File, PrintWriter}

import gov.pnnl.stm.conf.STMConf

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

/**
 * @author puro755
 *
 */
object writeMotifPatterns {


	def main(args: Array[String]): Unit = {
       val atomocMotif = STMConf.atomocMotif

 		    val t1=System.nanoTime()
			  println("Type prefix_annotation without space: ")
			  val scanner = new java.util.Scanner(System.in)
			  val prefix_annotation = scanner.nextLine()
  
        //val outFile = new PrintWriter(new File(t1+"Motifs_JSON_"+prefix_annotation+".txt"))
    		/*
    		 * Read Probability file and create an Array of probs
    		 */
    // val probFile = "/Users/puro755/OneDrive - " +
		//								 "PNNL/PhD/GraphGeneration/STM/325830731882378MProbNOverlap_regenerated_AlphaBitCoinT1.txt"
		val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/STM/26030554374972MProbNOverlap_regenerated_test.txt"

		//val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/332467846836844MProbNOverlap_regenerated_tweetTimeOrderingTry2.txt"//Added Time Ordering Original File for twitter Mention Graph
    		
        	
        	
        val OffsetFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/Offset.txt"
    		val motifProbability : scala.collection.mutable.ListBuffer[Double] =  scala.collection.mutable.ListBuffer()
    		val motifOffset : scala.collection.mutable.ListBuffer[Long] =  scala.collection.mutable
          .ListBuffer()
				
    		val outFile = new PrintWriter(new File(probFile.replaceAll("MProbNOverlap_regenerated_", "MProb")+"Motifs_JSON_"+prefix_annotation+".txt"))
    		
    		
    		for (line <- Source.fromFile(probFile).getLines) {
				  if(line.startsWith("duration") == false)  
				  	motifProbability += line.trim().toDouble
				}
    		for (line <- Source.fromFile(OffsetFile).getLines) {
				  //  motifOffset += ( (0,0,line.trim().split(",")(0).toInt,line.trim().split(",")(1).toInt))
          motifOffset += ( line.trim().toLong)
				    //two extra 0s added to make it similar to add min/max offset of each edge
				}
				//writeJSON(atomocMotif,outFile,motifProbability,motifOffset,0,0)
    		outFile.flush()
  }

	def writeJSON(atomocMotif: Array[String], outFile: PrintWriter, motifProbability: ListBuffer[Double],
                motifOffset: ListBuffer[Long],duration:Long,v_size:Long) =
	{


		val rn = Random
		var cnt = -1
    /*
     * Write non-pattern data
     */
		outFile.println("{")
		outFile.println("\"duration\" : \"" + duration + "\",")
		outFile.println("\"graph_size\" : \"" + v_size + "\",")
		outFile.println("\"patterns\" : [" )
    // Write multi-edgge
		for(i <- 0 to 0)
		{

			val numberOfNodesInMotif = 2
			val baseCnt = 7 // multi-edge is 8th motif
			val motif = atomocMotif(baseCnt)
			cnt = cnt + 1 // now cnt is 0
			outFile.println("{")
			outFile.println("\"id\": \"M" + cnt + "\",")
			outFile.println("\"track\": \"false\",")
			outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
			outFile.println("\"vertices\": [")
			outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
			outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			outFile.println("],")
			outFile.println("\"edges\": [")
			val allEdges = motif.split(";")
      //baseCnt MUST be fixed It is 8th motif but written at th top of the node-reuse and offset file
			//val thisEdgeTimeArray  = motifOffset(baseCnt).productIterator.toArray
      val thisEdgeTimeArray  = motifOffset(0)//.productIterator.toArray
      // productIterator converts tuple to a list
      // for mutli-edge we dont want to write two edges because by definition it is an extra
      // edge
			for (j <- 0 to allEdges.length - 2) {
				val edge = allEdges(j)
				//"(a)-[e1]->(b); (b)-[e2]->(a)",
				val edgeArr = edge.split("-")
				val src = edgeArr(0).replace("(", "").replace(")", "").trim()
				val dst = edgeArr(2).replace("(", "").replace(")", "").replace(">", "").trim()
        // min offset is 0, max is avg time we get from file
				val edgeMinOffset :Int = 0
				val edgeMaxOffset = 0//thisEdgeTimeArray(j).toString.toInt
				//We get this from (0,0,t1,t2) OLD

				if (j == allEdges.length - 2)
					outFile.println("{\"id\": \"" + src + dst + "\", \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}}")
				else
					outFile.println("{\"id\": \"" + src + dst + "\" , \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}},")
			}
			outFile.println("]")
			outFile.println("},")

		}
		for(i <- 1 to 8) //frist write 2 triand
		{
      val diving_factor = 5
			val baseCnt = i/diving_factor
			val motif = atomocMotif(baseCnt)
			cnt = cnt + 1
			outFile.println("{")
			outFile.println("\"id\": \"M" + cnt + "\",")
			outFile.println("\"track\": \"false\",")
			outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
			outFile.println("\"vertices\": [")
			if (cnt % 4 == 1) {
				outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt % 4 == 2) {
				// One of the node must be reused
				val randomReuse = rn.nextInt(3)
				if (randomReuse == 0)
					outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				if (randomReuse == 1)
					outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				if (randomReuse == 2)
					outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt % 4 == 3) {
				val onlyNewNode = rn.nextInt(3) // this condition is opposite to the one above. We check "onlyNewNode",
				// where as above we check "randomReuse" node
				if (onlyNewNode == 0)
					outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")

				if (onlyNewNode == 1)
					outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")

				if (onlyNewNode == 2)
					outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt % 4 == 0) {
				outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			}

			outFile.println("],")
			outFile.println("\"edges\": [")
			val allEdges = motif.split(";")
			val thisEdgeTimeArray  = motifOffset(baseCnt)//.productIterator.toArray
      for (j <- allEdges.indices) {
				val edge = allEdges(j)
				//"(a)-[e1]->(b); (a)-[e2]->(b); (b)-[e3]->(c)",
				val edgeArr = edge.split("-")
				val src = edgeArr(0).replace("(", "").replace(")", "").trim()
				val dst = edgeArr(2).replace("(", "").replace(")", "").replace(">", "").trim()
				val edgeMinOffset :Int = 0//thisEdgeTimeArray(j-1).toString.toInt
				val edgeMaxOffset = 0//thisEdgeTimeArray(j).toString.toInt
				//We get this from (0,0t1,t2)

				if (j == allEdges.length - 1)
					outFile.println("{\"id\": \"" + src + dst + "\", \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}}")
				else
					outFile.println("{\"id\": \"" + src + dst + "\" , \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}},")
			}
			outFile.println("]")
			outFile.println("},")
		}

		for(i <- 9 to 16) //Now write 2 edge non-loop ab ac
		{
			val numberOfNodesInMotif = 3
			val baseCnt = if(i<=12) 2 else 3
			val motif = atomocMotif(baseCnt)
			cnt = cnt + 1
			outFile.println("{")
			outFile.println("\"id\": \"M" + cnt + "\",")
			outFile.println("\"track\": \"false\",")
			outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
			outFile.println("\"vertices\": [")
			//if (cnt % 4 == 0)  TODO: Get a better logic
			if ((cnt == 9)|| (cnt == 13)) {
				outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} //else if (cnt % 4 == 1)
			else if ((cnt == 10)|| (cnt == 14) ){
				// One of the node must be reused
				val randomReuse = rn.nextInt(numberOfNodesInMotif)
				if (randomReuse == 0)
					outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				if (randomReuse == 1)
					outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				if (randomReuse == 2)
					outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if ((cnt == 11) || (cnt ==15)) {

				val onlyNewNode = rn.nextInt(numberOfNodesInMotif) // this condition is opposite to the one above. We check "onlyNewNode",
				// where as above we check "randomReuse" node
				if (onlyNewNode == 0)
					outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")

				if (onlyNewNode == 1)
					outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")

				if (onlyNewNode == 2)
					outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")

			}
			else if ((cnt == 12)|| (cnt == 16)) {
				outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			}

			outFile.println("],")
			outFile.println("\"edges\": [")
			val allEdges = motif.split(";")
			val thisEdgeTimeArray  = motifOffset(baseCnt)//.productIterator.toArray
      for (j <- allEdges.indices) {
				val edge = allEdges(j)
				//"(a)-[e1]->(b); (b)-[e2]->(a)",
				val edgeArr = edge.split("-")
				val src = edgeArr(0).replace("(", "").replace(")", "").trim()
				val dst = edgeArr(2).replace("(", "").replace(")", "").replace(">", "").trim()
				val edgeMinOffset :Int = 0//thisEdgeTimeArray(j-1).toString.toInt
				val edgeMaxOffset = 0//thisEdgeTimeArray(j).toString.toInt
				//We get this from (0,0t1,t2)

				if (j == allEdges.length-1)
					outFile.println("{\"id\": \"" + src + dst + "\", \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}}")
				else
					outFile.println("{\"id\": \"" + src + dst + "\" , \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}},")
			}
			outFile.println("]")
			outFile.println("},")
		}
		for(i <- 17 to 19)
		{

			val numberOfNodesInMotif = 2
			val baseCnt = 4
			val motif = atomocMotif(baseCnt)
			cnt = cnt + 1
			outFile.println("{")
			outFile.println("\"id\": \"M" + cnt + "\",")
			outFile.println("\"track\": \"false\",")
			outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
			outFile.println("\"vertices\": [")
			if (cnt == 17) {
				outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt == 18) {
				// One of the node must be reused
				val randomReuse = rn.nextInt(numberOfNodesInMotif)
				if (randomReuse == 0)
					outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				if (randomReuse == 1)
					outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt == 19) {
				outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			}

			outFile.println("],")
			outFile.println("\"edges\": [")
			val allEdges = motif.split(";")
			val thisEdgeTimeArray  = motifOffset(baseCnt)//.productIterator.toArray
			for (j <- allEdges.indices) {
				val edge = allEdges(j)
				//"(a)-[e1]->(b); (b)-[e2]->(a)",
				val edgeArr = edge.split("-")
				val src = edgeArr(0).replace("(", "").replace(")", "").trim()
				val dst = edgeArr(2).replace("(", "").replace(")", "").replace(">", "").trim()
				val edgeMinOffset :Int = 0//thisEdgeTimeArray(j-1).toString.toInt
				val edgeMaxOffset = 0//thisEdgeTimeArray(j).toString.toInt
				//We get this from (0,0t1,t2)

				if (j == allEdges.length - 1)
					outFile.println("{\"id\": \"" + src + dst + "\", \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}}")
				else
					outFile.println("{\"id\": \"" + src + dst + "\" , \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}},")
			}
			outFile.println("]")
			outFile.println("},")

		}
		for(i <- 20 to 22) //Now write single edge
		{
			val numberOfNodesInMotif = 2
			val baseCnt = 5 // we are writing 6th motif
		val motif = atomocMotif(baseCnt)
			cnt = cnt + 1
			outFile.println("{")
			outFile.println("\"id\": \"M" + cnt + "\",")
			outFile.println("\"track\": \"false\",")
			outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
			outFile.println("\"vertices\": [")
			if (cnt == 20) { // 11 just a number to get 0,1,2 remainder
				outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt == 21 ) {
				// One of the node must be reused
				val randomReuse = rn.nextInt(numberOfNodesInMotif)
				if (randomReuse == 0)
					outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				else
					outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
				if (randomReuse == 1)
					outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
			} else if (cnt == 22 ) {
				outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
				outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			}

			outFile.println("],")
			outFile.println("\"edges\": [")
			val allEdges = motif.split(";")
			val thisEdgeTimeArray  = motifOffset(baseCnt)//.productIterator.toArray
			for (j <- allEdges.indices) {
				val edge = allEdges(j)
				//"(a)-[e1]->(b); (b)-[e2]->(a)",
				val edgeArr = edge.split("-")
				val src = edgeArr(0).replace("(", "").replace(")", "").trim()
				val dst = edgeArr(2).replace("(", "").replace(")", "").replace(">", "").trim()
				val edgeMinOffset :Int = 0//thisEdgeTimeArray(j-1).toString.toInt
				val edgeMaxOffset = 0//thisEdgeTimeArray(j).toString.toInt
				//We get this from (0,0t1,t2)

				if (j == allEdges.length - 1)
					outFile.println("{\"id\": \"" + src + dst + "\", \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}}")
				else
					outFile.println("{\"id\": \"" + src + dst + "\" , \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"1\"}},")
			}
			outFile.println("]")
			if(cnt==22)
					outFile.println("}")
      else
					outFile.println("},")


		}
		outFile.println("]")//patterns ends
		outFile.println("}")//json file ends
    outFile.flush()
	}

}
/*
  		//val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/STM_Probs.csv"
    		//val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/MProbNOverlap.txt"
    		//val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/MProbNOverlap.bkp.txt"
    		//val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/51197075728631MProbNOverlap_regenerated_20fileTry1.txt"
    		//val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/264752948030501MProbNOverlap_regenerated_testingMissinbV.txt"//this is the one looks best so far
    		//val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/319467168754955MProbNOverlap_regenerated_tweetTempoFeb1.txt"//Original File for twitter Mention Graph
        //val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/47502888602354MProbNOverlap_regenerated_lowAutoPrefAttachTry2.txt"
        //val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/87657156178903MProbNOverlap_regenerated_testNewMotifs1.txt"
        //val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/STM/439684679361677MProbNOverlap_regenerated_CollegeMsg-etype.txtMay4Try2.txt"
        //val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/STM/169114035205239MProbNOverlap_regenerated_ColegMsgT5.txt"
        //val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/STM/180270716572353MProbNOverlap_regenerated_CollegeMsgT6.txt"

 */