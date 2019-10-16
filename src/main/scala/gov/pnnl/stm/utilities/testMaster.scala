package gov.pnnl.stm.utilities
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object testMaster {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value").master("local")
      .getOrCreate()

// For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val d=spark.sparkContext.parallelize(Seq((1,3),(2,4),(20,44),(24,41))).toDF

// Displays the content of the DataFrame to stdout
    d.show()

    val res = d.where("(_1 - _2) < -10")
    res.show()
  }

}
