/**
 *
 * @author puro755
 * @dJan 2, 2018
 * @Mining
 */
package gov.pnnl.stm.algorithms
import java.io.File
import java.io.PrintWriter
import scala.util.Random
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

/**
 * @author puro755
 *
 */
object writeMotifPatternsNonOverlappingMultiTypes {

	 val etypes = Array(0,1)
	 
  def main(args: Array[String]): Unit = {
       val atomocMotif = Array("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)",
			"(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(c)",
			"(a)-[e1]->(b); (a)-[e2]->(c)",
			"(b)-[e1]->(a); (c)-[e2]->(a)",
			"(a)-[e1]->(b); (b)-[e2]->(a)",
			"(a)-[e1]->(b)")
			
 		    val rn = Random
 		    val t1=System.nanoTime()
			  println("Type prefix_annotation without space: ")
			  val scanner = new java.util.Scanner(System.in)
			  val prefix_annotation = scanner.nextLine()
  
        val outFile = new PrintWriter(new File(t1+"motifPattern_nonOverlapping"+prefix_annotation+".txt"))
    		/*
    		 * Read Probability file and create an Array of probs
    		 */
        //val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/87657156178903MProbNOverlap_regenerated_testNewMotifs1.txt"
        //val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/22274075983230779MProbNOverlap_regenerated_testMType1MEdges.txt"
        //val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/93147033366182MProbNOverlap_regenerated_allPhoneEmailWithEType20kTry2Original.txt"
        //val probFile = "/sumitData/work/myprojects/AIM/branch_master/Mining/CompositePhoneEmail2MEdgesHadoop04FixedSingleEdge2NewNodesProb.txt"
        val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/STM/375791876725765MProbNOverlap_regenerated_phoneEmail20KT1.txt"
    		val OffsetFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/Offset.txt"
    		val motifProbability : scala.collection.mutable.ArrayBuffer[Double] =  scala.collection.mutable.ArrayBuffer() 
    		val motifOffset : scala.collection.mutable.ArrayBuffer[(Int,Int,Int,Int)] =  scala.collection.mutable.ArrayBuffer()
				for (line <- Source.fromFile(probFile).getLines) {
						if(line.startsWith("duration") == false)  
				    motifProbability += line.trim().toDouble
				}
    		for (line <- Source.fromFile(OffsetFile).getLines) {
				    motifOffset += ( (0,0,line.trim().split(",")(0).toInt,line.trim().split(",")(1).toInt))
				    //two extra 0s added to make it simler to add min/max offset of each edge
				}
    		var cnt = -1
    		//for(i <- 0 to allMotifs.length-1)
    		for(i <- 0 to 63) //first write 2 triangles
		    {
    		  val baseCnt = 0
    		  val motif = atomocMotif(baseCnt)
		      cnt = cnt + 1
		      outFile.println("{")
		      outFile.println("\"id\": \"M" + cnt + "\",")
		      outFile.println("\"track\": \"true\",")
		      outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
		      outFile.println("\"vertices\": [")
		      /*
		       * 0 to 7 are all new motifs
		       * 8 to 15 : 1 node reused
		       * 16 to 23 : 2 nodes reused
		       * 24 to 31 : all 3 nodes reused
		       */
		      
		      if (cnt % 4 == 0) {// 0th temporal motif OR 4th
		        outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
		      } else if (cnt % 4 == 1) {
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
		      } else if (cnt % 4 == 2) {
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
		      } else if (cnt % 4 == 3){
		        outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
		      }
		
		      outFile.println("],")
		      outFile.println("\"edges\": [")
		      val allEdges = motif.split(";")
		      val thisEdgeTimeArray  = motifOffset(baseCnt).productIterator.toArray
		      val allEdgeCombinations : ArrayBuffer[(String, String, String)] = ArrayBuffer.empty
					for (et1 <- 0 to etypes.length - 1) {
						for (et2 <- 0 to etypes.length - 1) {
							for (et3 <- 0 to etypes.length - 1) {
								allEdgeCombinations.append((etypes(et1).toString,etypes(et2).toString,etypes(et3).toString)) 
							}
						}
					}	
		      
			if (cnt <= 31) {
				val motifEdgeTypes = allEdgeCombinations(cnt/4)
				//cyclic triangle
				outFile.println("{\"id\": \"" + "ab" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "b" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._1 + "\"}},")
				outFile.println("{\"id\": \"" + "bc" + "\", \"source\": \"" + "b" + "\", \"target\": \"" + "c" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._2 + "\"}},")
				outFile.println("{\"id\": \"" + "ca" + "\", \"source\": \"" + "c" + "\", \"target\": \"" + "a" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._3 + "\"}}")
			}
			else if (cnt <= 63) {
				val motifEdgeTypes = allEdgeCombinations((cnt/4) - 8)
				//non-cyclic triangle
				outFile.println("{\"id\": \"" + "ab" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "b" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._1 + "\"}},")
				outFile.println("{\"id\": \"" + "bc" + "\", \"source\": \"" + "b" + "\", \"target\": \"" + "c" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._2 + "\"}},")
				outFile.println("{\"id\": \"" + "ac" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "c" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._3 + "\"}}")
		      }
		      
		      outFile.println("]")
		      outFile.println("},")
		  }
    		
    	for(i <- 64 to 95) //Now write 2 edge non-loop ab ac; 16 + 16 typed temporal variations
		    {
    			val numberOfNodesInMotif = 3
    		  val baseCnt = 2 
    		  val motif = atomocMotif(baseCnt)
		      cnt = cnt + 1
		      outFile.println("{")
		      outFile.println("\"id\": \"M" + cnt + "\",")
		      outFile.println("\"track\": \"true\",")
		      outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
		      outFile.println("\"vertices\": [")
		      //if (cnt % 4 == 0) { TODO: Get a better logic 
		      if (cnt %4 ==0) { //9th and 13th temporal motif
		        outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"c\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
		      } //else if (cnt % 4 == 1) {
		      else if (cnt %4 ==1){
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
		      }else if (cnt %4 ==2){
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
					outFile.println("{\"id\": \"c\", \"new\": \"ture\", \"attributes\": {\"label\": \"motivator\"}}")
				else
					outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
			}
			else if (cnt %4 ==3) { // 112h and 16th temporal motif
		        outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
		      }
		
    			outFile.println("],")
		      outFile.println("\"edges\": [")
		      val allEdges = motif.split(";")
		      val thisEdgeTimeArray  = motifOffset(baseCnt).productIterator.toArray
		      val allEdgeCombinations : ArrayBuffer[(String, String)] = ArrayBuffer.empty
					for (et1 <- 0 to etypes.length - 1) {
						for (et2 <- 0 to etypes.length - 1) {
								allEdgeCombinations.append((etypes(et1).toString,etypes(et2).toString)) 
						}
					}

		       //allEdgeCombinations is a 4 size program
		      
			if (cnt <= 79) {
				val motifEdgeTypes = allEdgeCombinations((cnt/4) - 16)
				//cyclic triangle
				outFile.println("{\"id\": \"" + "ab" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "b" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._1 + "\"}},")
				outFile.println("{\"id\": \"" + "ac" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "c" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._2 + "\"}}")
			}
			else if (cnt <= 95) {
				val motifEdgeTypes = allEdgeCombinations((cnt/4) - 20)
				//non-cyclic triangle
				outFile.println("{\"id\": \"" + "ba" + "\", \"source\": \"" + "b" + "\", \"target\": \"" + "a" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._1 + "\"}},")
				outFile.println("{\"id\": \"" + "ca" + "\", \"source\": \"" + "c" + "\", \"target\": \"" + "a" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._2 + "\"}}")
		      }
		      outFile.println("]")
		      outFile.println("},")
		  }
    	
    		for(i <- 96 to 107)
    		{
    			
    			val numberOfNodesInMotif = 2
    		  val baseCnt = 4
    		  val motif = atomocMotif(baseCnt)
		      cnt = cnt + 1
		      outFile.println("{")
		      outFile.println("\"id\": \"M" + cnt + "\",")
		      outFile.println("\"track\": \"true\",")
		      outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
		      outFile.println("\"vertices\": [")
		      if (cnt%3 == 0) {
		        outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
		      } else if (cnt%3 == 1) {
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
		      } else if (cnt%3 == 2) {
		        outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
		      }
		
    			outFile.println("],")
		      outFile.println("\"edges\": [")
		      val allEdges = motif.split(";")
		      val thisEdgeTimeArray  = motifOffset(baseCnt).productIterator.toArray
		      val allEdgeCombinations : ArrayBuffer[(String, String)] = ArrayBuffer.empty
					for (et1 <- 0 to etypes.length - 1) {
						for (et2 <- 0 to etypes.length - 1) {
								allEdgeCombinations.append((etypes(et1).toString,etypes(et2).toString)) 
						}
					}
    			
		      val motifEdgeTypes = allEdgeCombinations((cnt/3) - 32) //allEdgeCombinations is a 4 size program
		      

				outFile.println("{\"id\": \"" + "ab" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "b" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes._1 + "\"}}")

		      outFile.println("]")
		      outFile.println("},")
		  
    		}
    	  for(i <- 108 to 113) //Now write single edge
		    {
    			val numberOfNodesInMotif = 2
    		  val baseCnt = 5 // we are writing 3rd motif 
    		  val motif = atomocMotif(baseCnt)
		      cnt = cnt + 1
		      outFile.println("{")
		      outFile.println("\"id\": \"M" + cnt + "\",")
		      outFile.println("\"track\": \"true\",")
		      outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
		      outFile.println("\"vertices\": [")
		      if (cnt %3 ==0) { // 11 just a number to get 0,1,2 remainder
		        outFile.println("{\"id\": \"a\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"true\", \"attributes\": {\"label\": \"motivator\"}}")
		      } else if (cnt %3 ==1 ) {
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
		      } else if (cnt %3 == 2 ) {
		        outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
		      }
		
		      outFile.println("],")
		      outFile.println("\"edges\": [")
		      val allEdges = motif.split(";")
		      val thisEdgeTimeArray  = motifOffset(baseCnt).productIterator.toArray
		      val allEdgeCombinations : ArrayBuffer[(String)] = ArrayBuffer.empty
					for (et1 <- 0 to etypes.length - 1) {
								allEdgeCombinations.append((etypes(et1).toString)) 
					}
    			
		      val motifEdgeTypes = allEdgeCombinations((cnt/3) - 36) //allEdgeCombinations is a 3 size program
		      
				//cyclic triangle
				outFile.println("{\"id\": \"" + "ab" + "\", \"source\": \"" + "a" + "\", \"target\": \"" + "b" + "\", \"directed\": \"true\", \"minOffset\": \"" + 0 + "\", \"maxOffset\": \"" + 100 + "\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"" + motifEdgeTypes + "\"}}")

		      outFile.println("]")
		      outFile.println("},")
		  
		  }
    		outFile.flush()
  }

}