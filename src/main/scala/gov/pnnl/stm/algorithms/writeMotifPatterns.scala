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

/**
 * @author puro755
 *
 */
object writeMotifPatternsNonOverlapping {

  def main(args: Array[String]): Unit = {
    val allMotifs = Array("(a)-[e1]->(b); (c)-[e2]->(b); (a)-[e3]->(b)",
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
 		    val rn = Random
        val outFile = new PrintWriter(new File("motifPattern2.txt"))
    
    
    		/*
    		 * Read Probability file and create an Array of probs
    		 */
    		//val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/STM_Probs.csv"
    		val probFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/MProb.txt"
    		val OffsetFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/Offset.txt"
    		val motifProbability : scala.collection.mutable.ArrayBuffer[Double] =  scala.collection.mutable.ArrayBuffer() 
    		val motifOffset : scala.collection.mutable.ArrayBuffer[(Int,Int,Int,Int)] =  scala.collection.mutable.ArrayBuffer()
				for (line <- Source.fromFile(probFile).getLines) {
				    motifProbability += line.trim().toDouble
				}
    		for (line <- Source.fromFile(OffsetFile).getLines) {
				    motifOffset += ( (0,0,line.trim().split(",")(0).toInt,line.trim().split(",")(1).toInt))
				    //two extra 0s added to make it simler to add min/max offset of each edge
				}
    		var cnt = -1
    		//for(i <- 0 to allMotifs.length-1)
    		for(i <- 0 to 127)
		    {
    		  val baseCnt = i/4
    		  val motif = allMotifs(baseCnt)
		      cnt = cnt + 1
		      outFile.println("{")
		      outFile.println("\"id\": \"M" + cnt + "\",")
		      outFile.println("\"track\": \"true\",")
		      outFile.println("\"probability\" :\"" + motifProbability(cnt) + "\",")
		      outFile.println("\"vertices\": [")
		      if (cnt % 4 == 0) {
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
		        val onlyNewNode = rn.nextInt(3)
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
		      } else if (cnt % 4 == 3) {
		        outFile.println("{\"id\": \"a\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"b\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}},")
		        outFile.println("{\"id\": \"c\", \"new\": \"false\", \"attributes\": {\"label\": \"motivator\"}}")
		      }
		
		      outFile.println("],")
		      outFile.println("\"edges\": [")
		      val allEdges = motif.split(";")
		      val thisEdgeTimeArray  = motifOffset(baseCnt).productIterator.toArray
		      for (j <- 1 to allEdges.length) {
		        val edge = allEdges(j-1)
		        //"(a)-[e1]->(b); (a)-[e2]->(b); (b)-[e3]->(c)",
		        val edgeArr = edge.split("-")
		        val src = edgeArr(0).replace("(", "").replace(")", "").trim()
		        val dst = edgeArr(2).replace("(", "").replace(")", "").replace(">", "").trim()
		        val edgeMinOffset :Int = thisEdgeTimeArray(j-1).toString.toInt
		        val edgeMaxOffset = thisEdgeTimeArray(j).toString.toInt
		        //We get this from (0,0t1,t2) 
		        
		        if (j == allEdges.length)
		          outFile.println("{\"id\": \"" + src + dst + "\", \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"leadership\"}}")
		        else
		          outFile.println("{\"id\": \"" + src + dst + "\" , \"source\": \"" + src + "\", \"target\": \"" + dst + "\", \"directed\": \"true\", \"minOffset\": \""+edgeMinOffset+"\", \"maxOffset\": \""+edgeMaxOffset+"\", \"streamNum\": \"1\", \"attributes\": {\"label\": \"leadership\"}},")
		      }
		      outFile.println("]")
		      outFile.println("},")
		  }
    		
    		outFile.flush()
  }

}