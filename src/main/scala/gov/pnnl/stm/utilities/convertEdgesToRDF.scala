/**
 *
 * @author puro755
 * @dAug 14, 2017
 * @Mining
 */
package gov.pnnl.stm.utilities

import java.io.PrintWriter
import java.io.File
import java.util.Scanner

/**
 * @author puro755
 *
 */
object convertEdgesToRDF {

  val mention = "0"
  val comm = "1"
  val sell = "2"
  val buy = "3"

    def main(args: Array[String]): Unit = {
    var inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C1.edges"
  	var outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C1.rdf"
  	//writeRDF(inputfile, outfile, mention)

  	
  	inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C1-sig.edges"
  	outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C1-sig.rdf"
  	//writeRDF(inputfile, outfile, mention)

  	inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C2.edges"
  	outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C2.rdf"
  	//writeRDF(inputfile, outfile, comm)

  	inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C2-sig.edges"
  	outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C2-sig.rdf"
  	//writeRDF(inputfile, outfile, comm)
  	
  	
  	inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3.edges"
  	outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3.rdf"
  	var cutOffID = 259727
  	//writeRDFProcurement(inputfile, outfile, cutOffID)
    
    inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3-sig.edges"
  	outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3-sig.rdf"
  	cutOffID = 261389
  	//writeRDFProcurement(inputfile, outfile, cutOffID)


  	inputfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3-sig.edges"
  	outfile = "/Volumes/Darpa-SDGG/15Aug2017Deliverables/C3-sig2.edges"
  	cutOffID = 261389
  	//writeRDFProcurement(inputfile, outfile, cutOffID)
  	writeEdgeFromRDF(inputfile, outfile, cutOffID)
  	
  }

  def writeEdgeFromRDF(inputfile:String,outfile:String,cutOffId:Int)
  {
     val op = new PrintWriter(new File(outfile))
    val input = new Scanner(new File(inputfile));
    while(input.hasNextLine())
  	{
      val line = input.nextLine()
      val arr = line.split("\t")
      var a = 0
      if((arr(0).toInt <= cutOffId) && (arr(1).toInt <= cutOffId))
        op.println(arr(0) +"\t" +(arr(1).toInt + cutOffId).toString()+"\t" +arr(2))
      else if((arr(0).toInt > cutOffId) && (arr(1).toInt <= cutOffId))
        op.println(arr(0).toInt - cutOffId +"\t" +(arr(1).toInt + cutOffId).toString+"\t" +arr(2))
        else if((arr(0).toInt <= cutOffId) && (arr(1).toInt > cutOffId))
          op.println(arr(0) +"\t" +arr(1) + "\t" +arr(2))
        else
          println("Invalid triple", line)

  	}
  }
  
  def writeRDFProcurement(inputfile:String,outfile:String,cutOffId:Int)
  {
    val op = new PrintWriter(new File(outfile))
    val input = new Scanner(new File(inputfile));
    while(input.hasNextLine())
  	{
      val line = input.nextLine()
      val arr = line.split("\t")
      if((arr(0).toInt <= cutOffId) && (arr(1).toInt > cutOffId))
        op.println(arr(0) +"\t" + sell +"\t" +arr(1) +"\t" +arr(2))
      else if((arr(0).toInt > cutOffId) && (arr(1).toInt <= cutOffId))
        op.println(arr(0) +"\t" + buy +"\t" +arr(1) +"\t" +arr(2))
        else
          println("Invalid triple", line)
  	}
    op.flush()
  }
  
  def writeRDF(inputfile:String,outfile:String,edgeType:String)
  {
    	  
      val op = new PrintWriter(new File(outfile))
      val input = new Scanner(new File(inputfile));
      while(input.hasNextLine())
  	  {
        val line = input.nextLine()
        val arr = line.split("\t")
        op.println(arr(0) +"\t" + edgeType +"\t" +arr(1) +"\t" +arr(2))
  	  }

      op.flush()  

  }
}