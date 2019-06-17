package gov.pnnl.stm.utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object testMaster {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("Test Master cores")
      .set("spark.rdd.compress", "true")
      .set("spark.shuffle.blockTransferService", "nio")
      .set("spark.serializer",
           "org.apache.spark.serializer.KryoSerializer")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sc = new SparkContext( sparkConf )

    val arr = Array.fill[Int](5000000)(1)

    val total = arr.reduce((v1,v2) => v1+v2)
    println("master total add is ", total)

    val arr2 = Array.fill[Int](5000000)(1)

    val total2 = arr2.reduce((v1,v2) => v1*v2)
    println("master total is ", total2)


  }

}
