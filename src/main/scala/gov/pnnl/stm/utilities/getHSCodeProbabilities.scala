/**
 *
 * @author puro755
 * @dFeb 27, 2018
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import java.io.File
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author puro755
 *
 */
object getHSCodeProbabilities {

  val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
  val sc = SparkContextInitializer.getSparkContext(sparkConf)
	def main(args: Array[String]): Unit = {
//		val nodeFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/HSCodeListLowAuto.csv"
				//val nodeFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/HSCodeListIMP2010.csv"
				val nodeFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/AllHSCodePIERS2013IMP.txt"

			val hsCodeRDD: RDD[(Int,Long)] =
      sc.textFile(nodeFile).map { line =>
      	//val lineArray = line.split(" ")
      	(line.toInt, 1L)
      }.reduceByKey((cnt1, cnt2)=>cnt1+cnt2)
      val sortedHSCodeRdd = hsCodeRDD.sortBy(_._2, false)
      
      val level4sortedHSCodeRdd = sortedHSCodeRdd.map(f=>if(f._1.toString.length() > 3)
      	(f._1.toString.substring(0, 4), (f._2,Set(f._1+"="+f._2))) // (1234,(653,Set(123456=653)))
      	else
      		(f._1,(f._2,Set(f._1+"="+f._2)) )   ).reduceByKey((s1,s2) => (s1._1 + s2._1, s1._2 ++ s2._2)).sortBy(_._2._1, false)
      		//sort it by the total value of 4Char-HSCODE
      //println(sortedHSCodeRdd.count)		
      
      val detailedOPRDD = level4sortedHSCodeRdd.map(f=>f._1+"="+f._2._1+","+f._2._2.toList.mkString(","))
      //8703=47755,870390=47719,870310=33,870321=2,870323=1
      detailedOPRDD.saveAsTextFile(nodeFile+"DetailedOP")
      
      /*
       * get average number of items in a HSCODE class with standard deviation
       */
      val itemCount = level4sortedHSCodeRdd.map(hcls=>hcls._2._2.size)
      val avg = itemCount.mean;
      val std = itemCount.stdev
      println("Avg Number of Items in each Class", avg)
      println("Std. Dev. = ", std)
      
      /*
       * get name of HSCODE class:
       */
      val hscodeDescFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/CopyHSCODES.txt"
      val hsCodeDescMap =
      sc.textFile(hscodeDescFile).map { line =>
      	//println(line)
      	val lineArray = line.split("\t")
      	if(lineArray(0).length() == 4)
      		(lineArray(0), lineArray(1).substring(0,Math.min(15, lineArray(1).length())).replaceAll("\"", "").replaceAll(",", ""))
      	else
      	(" ", " ")
      }.collectAsMap
      val matlabPlotOPRDD = level4sortedHSCodeRdd.map(f
      		=> hsCodeDescMap.getOrElse(f._1.toString, f._1.toString)+","+f._2._2.map(leafHSCodeEntry=>leafHSCodeEntry.split("=")(1)).toList.mkString(","))
      matlabPlotOPRDD.saveAsTextFile(nodeFile+"MatlabOP")
      //8703,47719,33,2,1
      //(Level 1,8703,(47755,Set(870390, 870310, 870321, 870323)))
      //47719 + 33 + 2 + 1
	}
/* V3 code
 * 
		val nodeFile = "/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/PIERS_RealWorld/FMAIN_to_HS_to_MAIN_ALL_HS_CO.edges"
		
			val hsCodeRDD: RDD[(Int,Long)] =
      sc.textFile(nodeFile).map { line =>
      	val lineArray = line.split(" ")
      	(lineArray(0).toInt, 1L)
      	
      }
		
		println("hs code distinct ", hsCodeRDD.count )
    val hscodeFrq = hsCodeRDD.reduceByKey((cnt1, cnt2)=>cnt1+cnt2)
    val totalHscodes = hsCodeRDD.count.toDouble
    val hscodeProb = hscodeFrq.map(hs => (hs._1, hs._2/totalHscodes)).sortBy(_._2,false)
    val outf = new PrintWriter(new File("/Users/puro755/OneDrive - PNNL/Projects/DARPA_MAA/code/shippingdataanalysis/PIERS_RealWorld/HSCodeDistribution.csv"))
    hscodeProb.collect.foreach(h => outf.println(h._1 + "," + h._2))
    outf.flush()
	
 */
}