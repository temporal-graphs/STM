/**
 *
 * @author puro755
 * @dMar 19, 2018
 * @Mining
 */
package gov.pnnl.stm.utilities

import org.apache.spark.rdd.RDD
import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * @author puro755
 *
 */
object getPhoneEmailDistributions {

	def main(args: Array[String]): Unit = {
		val sparkConf = SparkContextInitializer.getSparkConf().setAppName("DARPA-MAA STM")
		val sc = SparkContextInitializer.getSparkContext(sparkConf)
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1.March16.PhoneOnly"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDense-s1.March16.EmailOnly"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/compositePhoneEmail2MNodesDenseJune2018Syllogism1.0-s1April26.cleanEmailOnly"
		//val nodeFile = "/Users/puro755/OneDrive - PNNL/PhD/GraphGeneration/graph-stream-generator/June2018V4Release/compositePhoneEmail2MNodesDenseJune2018H9-s1April30.clean.EmailOnly"
		//val nodeFile = "/Users/puro755/OneDrive - " +
		//								"PNNL/PhD/GraphGeneration/graph-stream-generator/June2018V4Release" +
		//								 "/compositePhoneEmail2MNodesDenseJune2018H9-s1April30.clean.PhoneOnly"
		val nodeFile = "/Users/puro755/OneDrive - " +
									 "PNNL/PhD/GraphGeneration/graph-stream-generator/June2018V4Release/C1C2C3_V4_SylloMay7_G0Email.csv"
		val sep = ","
			/*
			 * Final Code used to get pairwise call distribution given to Jenny
			 * This is generic code for phone and email both. Correct file should be input
			 * SET fileType for correct output folder name
			 */
			val fileType = "Email"
			val twoPeoplePhone: RDD[((Int,Int),Int)] =
      sc.textFile(nodeFile).map { line =>
      	val lineArr = line.split(sep)
      	((math.min(lineArr(0).toInt,lineArr(2).toInt), math.max(lineArr(0).toInt,lineArr(2).toInt)),1)
      }
		val twoPeoplePhoneTotal = twoPeoplePhone.reduceByKey((cnt1,cnt2) => cnt1+cnt2)
		//now we get  ((sp ps),2000), ((sc,ka), 2000) ==> (2000, 2) 
		val twoPeoplePhoneDist = twoPeoplePhoneTotal.map(twoPplTotal => (twoPplTotal._2, 1)).reduceByKey((cnt1,cnt2) => cnt1+cnt2)
		twoPeoplePhoneDist.saveAsTextFile(nodeFile+"twoPeople"+fileType+"Dist")

System.exit(1)
			

			// get time duration between same set of people. USED in final dist calculation for Jenny
			
		val twoPeoplePhoneTime: RDD[((Int,Int),Set[Long])] =
      sc.textFile(nodeFile).map { line =>
      	val lineArr = line.split(sep)
      	((math.min(lineArr(0).toInt,lineArr(2).toInt), math.max(lineArr(0).toInt,lineArr(2).toInt)),Set(lineArr(3).toLong))
      }.reduceByKey((t1,t2) => t1++t2)
     
      val sortedtwoPeoplePhoneTime = twoPeoplePhoneTime.map(p=>(p._1, p._2.toList.sorted))
      val timeBwTwoPeoplePhone = sortedtwoPeoplePhoneTime.map(p=>(p._1, (p._2, p._2 drop 1).zipped.map(_-_)))
      val disttimeBwTwoPeoplePhone = timeBwTwoPeoplePhone.flatMap(p=>{
      	var allTimeDist : scala.collection.mutable.Set[(Long,Int)] = scala.collection.mutable.Set.empty
      	p._2.map(timeDiff => {
      		//allTimeDist += ((timeDiff/60, 1)) // used in disttimeBwTwoPeoplePhonePerMinute
      		allTimeDist += ((Math.abs(timeDiff/3660), 1)) //disttimeBwTwoPeoplePhonePerHour
      	})
      	allTimeDist
      }).reduceByKey((cnt1,cnt2) => cnt1+cnt2)
      disttimeBwTwoPeoplePhone.saveAsTextFile(nodeFile+fileType+"BwPairPerHourDist")
      //disttimeBwTwoPeoplePhone.saveAsTextFile("disttimeBwTwoPeople"+fileType+"PerMinute")

	}

}