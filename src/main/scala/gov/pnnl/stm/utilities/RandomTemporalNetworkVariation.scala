package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}

import scala.io.Source
import scala.util.Random

object RandomTemporalNetworkVariation {

  def main(args: Array[String]): Unit = {

    /*
     * Program specific configuration
     * clo is command_line_options
     */
    var clo: Map[String, String] = Map.empty
    for (arg <- args) {
      val kv = arg.split("=")
      clo += (kv(0) -> kv(1))
    }

    val sep: String =
      if (clo.getOrElse("-separator", ",").equalsIgnoreCase("\\t"))
        '\t'.toString
      else clo.getOrElse("-separator", ",")
    println("sep is " + sep)
    val baseGraph = clo.getOrElse("-input_file", "G0.csv")
    val numVariations = clo.getOrElse("-num_variations", "30").toInt


    for(v <- 1 to numVariations)
    {
      val SEED = 10000 * v
      val ran = new Random(SEED)
      val MAX_STRETCH = (86400 * v)
      val MUE :Int = MAX_STRETCH / 2 // one day
      val SIGMA = MAX_STRETCH/6
      val outFileName = "G"+v+".csv"
      val outPWr = new PrintWriter(new File(outFileName))
      println("######## Writing file " + outFileName)

      val source = Source.fromFile(baseGraph)
      for (line <- source.getLines())
      {
        val lineArr = line.split(sep)
        val gaussianStetch = ran.nextGaussian()*SIGMA+MUE
        val a = ran.nextDouble()
        val newTime =  (a * lineArr(3).toLong + gaussianStetch).toLong
//          if (ran.nextDouble() < .5)
//            (a * lineArr(3).toLong - gaussianStetch).toLong
//          else
//            (a * lineArr(3).toLong + gaussianStetch).toLong

        outPWr.println(lineArr(0) + sep + lineArr(1) + sep + lineArr(2) + sep + newTime)
      }
      source.close()
      outPWr.flush()
    }


  }

}
