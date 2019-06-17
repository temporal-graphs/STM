/**
 *
 * @author puro755
 * @dFeb 1, 2018
 * @Mining
 */
package gov.pnnl.stm.utilities

import scala.io.Source
import org.joda.time.format.DateTimeFormat
import java.io.PrintWriter
import java.io.File
import org.apache.spark.rdd.RDD
import gov.pnnl.builders.SparkContextInitializer
import gov.pnnl.builders.TAGBuilder

/**
 * @author puro755
 *
 */
object crateSecondBasedTemporalGraph {
	
	def main(args: Array[String]): Unit = {
		
		val inputfile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/LowEndAutosSmallNoType.csv"
		//val outFile = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_temporal_int_timeOrder.csv"))
		//val outFile = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/socialmediadata/intGraph/tweet_graph_temporal_int.csv"))
		val outFile = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/LowEndAutosSmallNoTypeDistinct.csv"))
		val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
		val sc = SparkContextInitializer.getSparkContext(sparkConf)
		val nodeQuadruples = TAGBuilder.init_rdd(inputfile,sc).tag_rdd
      
		println("nodeQuadruples size")
    val vRDD = nodeQuadruples.flatMap(nd=>Iterator((nd._1,nd._1),(nd._2,nd._2))).distinct
    println("number of v " , vRDD.count)
    println("number of edges " , nodeQuadruples.count)
    nodeQuadruples.collect.foreach(e=>outFile.println(e._1+" "+e._2+" "+e._3))
    outFile.flush()
    System.exit(1)
		
		for(line <- Source.fromFile(inputfile).getLines)  {
			val lineArr = line.split(" ")
			if( (lineArr.length == 3) &&  !lineArr(2).equals("Unknown") && (!lineArr(2).equals("Math") ) )
			{
				println(line)
				//val date = lineArr(2).split("/")
				//set to yyyymmdd order
				//val orderString = "%04d".format(date(2).toInt)+"%02d".format(date(0).toInt)+"%02d".format(date(1).toInt)
				val date = lineArr(2) + " 00:00:00.000"
				val f = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS");
				val dateTime = f.parseDateTime(date);
				val dateS = dateTime.getMillis() / 1000 // number of seconds
				
				//outFile.println(lineArr(0).hashCode() + " " + lineArr(1).hashCode() + " " + orderString)
				outFile.println(lineArr(0).hashCode() + " " + lineArr(1).hashCode() + " " + dateS)
			}
		}
		outFile.flush()
	}

}