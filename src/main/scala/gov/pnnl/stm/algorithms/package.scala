package gov.pnnl.stm

import gov.pnnl.builders.SparkContextInitializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

package object algorithms {

  /*
    *
    * Define all global variables for this class
    * NOTE: initializing gSC outside main method (i.e. at object initiliazation level gives
    * java.lang.NoClassDefFoundError: Could not initialize class gov.pnnl.stm.algorithms.STM_NodeArrivalRateMultiType
    *    .........
    *
    * because spark could not initialize multiple spark context for the singleton object
    */
  println("####Package object is created")
  //var sparkConf   : SparkConf     = null
  //var gSC         : SparkContext  = null
  //var gSQLContext : SQLContext    = null
  //var gETypes                                              = Array(12)
  // I dont know if we can change values of package members or not
}
