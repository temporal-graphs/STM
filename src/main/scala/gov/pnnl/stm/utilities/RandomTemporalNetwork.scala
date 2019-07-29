package gov.pnnl.stm.utilities
import java.io.{File, PrintWriter}


import scala.util.Random

object RandomTemporalNetwork {

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
      else clo.getOrElse("-separator", " ")
    println("sep is " + sep)
    val outFile = clo.getOrElse("-out_file", "GSmall0.csv")

    val population : Int = clo.getOrElse("-input_population","100").toInt
    val duration_sec : Int = clo.getOrElse("-input_duration_sec","86400").toInt
    val outPWr = new PrintWriter(new File(outFile))
    /*
     * get population
     */
    val population_rdd: Array[Int] = (0 to population-1).toArray

    val SEED = 10000
    val ran = new Random(SEED)

    population_rdd.foreach(v=>{
      for(t <- 0 to duration_sec)
      {
        if(ran.nextDouble() <= .01)
          {
            val dst = ran.nextInt(population)
            outPWr.println(v + sep + dst + sep + t.toLong)
          }
      }
      outPWr.flush()
    })

    outPWr.flush()
  }

}
