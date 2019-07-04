/**
  *
  * @author puro755
  * @dDec 24, 2017
  * @Mining
  */
package gov.pnnl.stm.algorithms

import java.io._
import java.nio.file.{Files, Paths}

import scalaz.Scalaz._
import util.control.Breaks._
import org.apache.spark.sql._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.graphframes.GraphFrame
import gov.pnnl.builders.{TAGBuilder}
import gov.pnnl.datamodel.GlobalTypes._
import gov.pnnl.stm.conf.STMConf
import org.apache.commons.io.filefilter.RegexFileFilter
import org.apache.spark.sql.functions.{col}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

/**
  * @author puro755
  *
  */
object STM_NodeArrivalRateMultiType {

  /*
   * Many global variables are define at the end of the file
   */

  println("######OBJECT CREATED ; STM_NodeArrivalRateMultiType ######")
  val t1 = System.nanoTime()
  val prefix_annotation = "kdd"
  val gMotifInfo = ListBuffer.empty[List[Int]]
  val gMotifOrbitInfo = ListBuffer.empty[List[Double]]
  val gOffsetInfo = ListBuffer.empty[List[Long]]
  //ALL THESE FILES ARE GETTING CREATED IN EACH EXECUTOR ALSO
  val gMotifProbFile = new File(
    t1 + "MotifProb_Rate_" + prefix_annotation + ".txt"
  )
  val gMotifProbFWriter = new PrintWriter(gMotifProbFile)
  val gMotifAllProbFile = new File(
    t1 + "MotifProb_AbsCount_" + prefix_annotation + ".txt"
  )
  val gMotifAllProbFWriter = new PrintWriter(gMotifAllProbFile)
  val gMotifOrbitFile = new File(
    t1 + "MotifOrbit_Independence_" + prefix_annotation + ".txt"
  )
  val gMotifOrbitFWriter = new PrintWriter(gMotifOrbitFile)
  val gMotifAllProbFile_Individual = new File(
    t1 + "MotifProb_AbsCount_Individual" +
      prefix_annotation + ".txt"
  )
  val gMotifAllProb_IndividualFWriter = new PrintWriter(
    gMotifAllProbFile_Individual
  )
  val gOffsetFile = new File(t1 + "Offset_Rate_" + prefix_annotation + ".txt")
  val gOffsetFWriter = new PrintWriter(gOffsetFile)
  val gOffsetAllFile = new File(
    t1 + "Offset_AbsCount_" + prefix_annotation + ".txt"
  )
  val gOffsetAllFWriter = new PrintWriter(gOffsetAllFile)
  val gVertexBirthFile = new File(
    t1 + "VertexBirth_" + prefix_annotation + ".txt"
  )
  val gVertexBirthFWriter = new PrintWriter(gVertexBirthFile)
  val gMotifIndependenceFile = new File(
    t1 + "Motif_Independence_" + prefix_annotation + ".txt"
  )
  val gMotifIndependenceFWriter = new PrintWriter(
    new FileWriter(gMotifIndependenceFile, true)
  )
  val gVertexIndependenceFile = new File(
    t1 + "Vertex_Independence_" + prefix_annotation + ".txt"
  )
  val gVertexIndependenceFWriter = new PrintWriter(
    new FileWriter(gVertexIndependenceFile, true)
  )
  val gOrbitVertexAssociationFile = new File(
    t1 + "Orbit_Association_" + prefix_annotation + ".txt"
  )
  val gOrbitVertexAssociationFWriter = new PrintWriter(
    new FileWriter(gOrbitVertexAssociationFile, true)
  )
  val gMotifVertexAssociationFile = new File(
    t1 + "Motif_Association_" + prefix_annotation + ".txt"
  )
  val gMotifVertexAssociationFWriter = new PrintWriter(
    new FileWriter(gMotifVertexAssociationFile, true)
  )

  val gHigherGraphFile = new File(
    t1 + "HigherGraph_" + prefix_annotation + "" + ".txt"
  )
  val gHigherGraphFWriter = new PrintWriter(
    new FileWriter(gHigherGraphFile, true)
  )
  val gDebug = true
  val gHigherGOut = false
  val gAtomicMotifs: Map[String, String] = STMConf.atomocMotif
  val gMotifKeyToName = STMConf.atomocMotifKeyToName
  val gMotifNameToKey = STMConf.atomocMotifNameToKey
  val gMotifNameToOrbitKeys = STMConf.motifNameToOrbitKeys


  var currItrID = 1
  var currWinID = 1
  /*
   * if we define gETypes here as "var" and then update it's value from command line. The new value does not reach to
   * the executor becuase driver has already sent the value to executor once and it does not -resend it when the
   * value is updated at driver (it's possible to broadcast it to update the values")
   * map(), flatmap(), filter() etc closure are run inside executors
   *
   * Ex; Initialized gETypes with Array(11). update the value in the driver. then right outside the .filter
   * command the value is "new" but within the filter it is Array11)
   *
   * https://stackoverflow.com/questions/31366467/how-spark-driver-serializes-the-task-that-is-sent-to-executors
   * In a REPL envirenment, spark compile the user code to class files and put on a file server, the executor
   * implements a custom classloader which load the class from the file server on the driver side; the class is
   * actually a function to run against a iterator of records
   *
   *
   * SO how do we get the upated values of gMotifInfo, gOffsetInfo etc...??? Because these are used only on the
   * driver JVM. partial results are received from the executor and the same global variable is updated on driver.
   */
  var gVBirthTime: scala.collection.mutable.Map[Int, Long] =
    scala.collection.mutable.Map.empty

  def writeAvgOutDegFile(avg_out_deg_fname: String,
                         avg_out_deg: Array[Double]): Unit = {
    val fwriter = new PrintWriter(new File(avg_out_deg_fname))
    fwriter.println(avg_out_deg.mkString("\n"))
    fwriter.flush()
  }

  /**
    * main function of the object
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    /*
     *
     * Define all global variables for this class
     * NOTE: initializing gSC outside main method (i.e. at object initiliazation level gives
     * java.lang.NoClassDefFoundError:
     * Could not initialize class gov.pnnl.stm.algorithms.STM_NodeArrivalRateMultiType
     *    .........
     *
     * because spark could not initialize multiple spark context for the singleton object
     */
    lazy val sparkConf = new SparkConf()
      .registerKryoClasses(Array.empty)
      .set("spark.driver.cores", "14")

    lazy val sparkSession = SparkSession
      .builder()
      .appName("DARPA-MAA STM")
      .config(sparkConf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = sparkSession.sparkContext
    val sqlc = sparkSession.sqlContext

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
    val nodeFile = clo.getOrElse("-input_file", "input-graph-file.csv")
    val avg_outdeg_file =
      clo.getOrElse("-avg_outdeg_file", nodeFile + "avg_outdeg.csv")

    val output_base_dir = clo.getOrElse("-base_out_dir", "./output/testout/")

    println("input paramters are :" + clo.toString)
    println("Spark paramters are ", sc.getConf.getAll.foreach(println))

    /*
     * Get the base tag rdd which has 4 things: src etype dst time
     *
     */
    val inputTAG = TAGBuilder.init_rdd(nodeFile, sc, sep)

    /*
     * Main method to get motif probability .It returns 3 results:
     *    * normMotifProb: normalized motif probability
     *    * offsetProb: time offset of the motifs
     *    * avg_out_deg: out degree distribution of the input graph
     */
    var local_res = processTAG(inputTAG, gDebug, clo)

    println("local res 1" + local_res._1)

    //write json file
    // val out_file_os_path = new PrintWriter(new File(out_json_file_os_path))
    //    writeMotifPatterns.writeJSON(gAtomicMotifs.values.toArray, out_file_os_path, local_res._1,
    //                                 local_res
    //      ._2,
    //                                 duration, v_size)

    //write average out degree file
    writeAvgOutDegFile(avg_outdeg_file, local_res._3)

    moveFilesToOutdir(output_base_dir)

  }

  def processTAG(
    baseTAG: gov.pnnl.datamodel.TAG,
    gDebug: Boolean,
    clo: Map[String, String]
  ): (ListBuffer[Double], ListBuffer[Long], Array[Double]) = {

    val nodeFile = clo.getOrElse("-input_file", "input-graph-file.csv")

    val out_json_file_os_path =
      clo.getOrElse("-out_json_file_os_path", "output-motif.json")

    val sampling: Boolean = clo.getOrElse("-sampling", "false").toBoolean

    val sampling_population: Int =
      clo.getOrElse("-sampling_population", "10").toInt

    val sample_selection_prob: Double =
      clo.getOrElse("-sample_selection_prob", "0.5").toDouble

    val num_iterations: Int = clo.getOrElse("-num_iterations", "3").toInt

    val gETypes =
      clo.getOrElse("-valid_etypes", "0").split(",").map(et => et.toInt)

    val inputSimpleTAG = baseTAG.get_simple_tagrdd
    /*
     * Broacast the vertext arrival times to each cluster-node because it us used in look-up as
     * local Map
     */
    val vAppearanceTime: RDD[(Int, Long)] =
      this.get_vertex_birth_time(inputSimpleTAG).cache()
    val vAppearanceTimeMap: scala.collection.mutable.Map[Int, Long] =
      scala.collection.mutable.Map(vAppearanceTime.collect(): _*)

    //write vertex birth time
    vAppearanceTimeMap.values.foreach(t => gVertexBirthFWriter.println(t))
    gVertexBirthFWriter.flush()
    gVBirthTime = vAppearanceTimeMap
    vAppearanceTime.unpersist(true)
    //https://stackoverflow.com/a/6628822/1413892

    //Write out degree per day file which is needed
//    val avg_out_deg = g.outDegrees
//      .map(row => (row.getAs[Int](1)).toDouble / duration_days)
//      .collect()
    val avg_out_deg = Array[Double]()
    if (sampling) {
      val res = approx_STM(
        gDebug,
        sampling_population,
        sample_selection_prob,
        num_iterations,
        gETypes,
        inputSimpleTAG
      )
      (res._1, res._2, avg_out_deg)
    } else {
      val res = complete_STM(gDebug, gETypes, inputSimpleTAG)
      (res._1, res._2, avg_out_deg)
    }

  }

  def findIsolatedVtx(g: GraphFrame,
                      motifName: String,
                      gETypes: Array[eType]): GraphFrame = {

    if (gDebug) {
      println("graph sizev ", g.vertices.count)
      println("graph size e", g.edges.count)
    }

    var isolated_v  = g.degrees
      .filter(v => v.getAs[Int](1) == 0)
      .rdd
      .map(v => v.getAs[Int](0))
      .cache()
    // for 7 -1 7 0 type of edges, degree will be 2 not . one out and one in degree

    if (isolated_v.count() == 0)
      isolated_v = g
        .filterEdges("type = -1")
        .dropIsolatedVertices()
        .vertices
        .rdd
        .map(v => v.getAs[Int](0))
        .cache()

    val iso_v_cnt = isolated_v.count
    println("iso count is ", iso_v_cnt)

    writeMotifVertexAssoication(isolated_v.collect(),motifName)

    write_vertex_independence(iso_v_cnt, iso_v_cnt)

    write_motif_independence(iso_v_cnt, iso_v_cnt)

    gMotifAllProb_IndividualFWriter.println("Iso_V", iso_v_cnt)
    gMotifInfo += List(iso_v_cnt.toInt)
    println(gMotifInfo)
    //gOffsetInfo += List(0L)
    g.filterEdges("type != -1").dropIsolatedVertices()
  }

  def get_edge_from_row(row: Row): (Int, Int, Int, Long) = {
    (get_row_src(row), get_row_etype(row), get_row_dst(row), get_row_time(row))
  }

  def findIsolatedEdg(g: GraphFrame,
                      motifName: String,
                      gETypes: Array[eType]): GraphFrame = {

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    import org.apache.spark.sql.functions._

    println("in isolated edge")
    if (gDebug) {
      println("graph sizev ", g.vertices.count)
      println("graph size e", g.edges.count)
    }
    val v_deg_1 = g.degrees.filter(row => row.getAs[Int](1) == 1).cache()

    // get_row_src is used use but it is just getting 0th element of the row
    val v_deg_1_id = v_deg_1.collect().map(row => get_row_src(row))
    val v_deg_1_exc_local = sc.broadcast(v_deg_1_id).value
    val iso_edgs = g
      .find(gAtomicMotifs(motifName))
      .filter(
        col("a.id").isin(v_deg_1_exc_local: _*)
          && col("b.id").isin(v_deg_1_exc_local: _*)
      )

    val selectEdgeArr = Array("e1.src", "e1.type", "e1.dst", "e1.time")
    val selctedMotifEdges =
      iso_edgs.select(selectEdgeArr.head, selectEdgeArr.tail: _*)
    val iso_edge_cnt = selctedMotifEdges.count()

    val tmi_edges_rdd: RDD[(Int, Int, Int, Long)] =
      selctedMotifEdges.rdd.map(row => get_edge_from_row(row))
    write_motif_vertex_association_file(tmi_edges_rdd,motifName)

    val newe = g.edges.except(selctedMotifEdges)
    val newv = g.vertices.except(v_deg_1)
    println("in 2 iso edge")

    write_vertex_independence(iso_edge_cnt * 2, iso_edge_cnt * 2)
    write_motif_independence(iso_edge_cnt, iso_edge_cnt)

    gMotifAllProb_IndividualFWriter.println("iso_e", iso_edge_cnt)
    gMotifInfo += List(iso_edge_cnt.toInt)
    //gOffsetInfo += List(0L)
    GraphFrame(newv, newe)
  }

  def findQuad(g: GraphFrame,
               motifName: String,
               gETypes: Array[eType]): GraphFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    var tmpG = g
    for (et1 <- gETypes.indices) {
      for (et2 <- gETypes.indices) {
        for (et3 <- gETypes.indices) {
          for (et4 <- gETypes.indices) {
            breakable {
              if (gDebug) {
                println("graph quad sizev ", g.vertices.count)
                println("graph size e", g.edges.count)
              }
              println("Motif is ", motifName)
              val validMotifsArray: RDD[(Int, Int, Int, Long)] =
                if (motifName
                      .equalsIgnoreCase("twoloop"))
                  find4EdgNVtxMotifs(
                    tmpG,
                    motifName,
                    et1,
                    et2,
                    et3,
                    et4,
                    gETypes,
                    3,
                    4
                  )
                else
                  find4EdgNVtxMotifs(
                    tmpG,
                    motifName,
                    et1,
                    et2,
                    et3,
                    et4,
                    gETypes,
                    4,
                    4
                  )

              //TODO: look at the need of this check and the return type
              if (validMotifsArray.isEmpty)
                break

              write_motif_vertex_association_file(validMotifsArray,motifName)
//              if (motifName.equalsIgnoreCase("quad"))
//                write_motif_vertex_association_file(validMotifsArray, "quad")
//              else if (motifName.equalsIgnoreCase("twoloop"))
//                write_motif_vertex_association_file(validMotifsArray, "twoloop")

              val uniqeEDF = sqlc
                .createDataFrame(validMotifsArray)
                .toDF("src", "type", "dst", "time")

              /*
               * 			dataFrame's except methods returns distinct edges by default.
               *      I dont see the documentation saying this. I have fixed the graph reader code and do a "distinct" while
               *      creating the base RDD
               */
              val newEDF = tmpG.edges.except(uniqeEDF)
              import sqlc.implicits._
              val newVRDD = newEDF
                .flatMap(
                  nd =>
                    Iterator(
                      (nd.getAs[Int](0), nd.getAs[Int](0)),
                      (nd.getAs[Int](2), nd.getAs[Int](2))
                  )
                )
                .distinct
                .toDF("id", "name")
              import sqlc.implicits._
              val newGraph = GraphFrame(newVRDD, newEDF)
              tmpG.unpersist(true)
              tmpG = newGraph.cache()
            }
          }
        }
      }
    }

    tmpG
  }

  def findAllITeM(gETypes: Array[eType],
                  call_id_val: Int,
                  initial_tag: SimpleTAGRDD): GraphFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    var call_id = call_id_val

    val vInitialRDD = initial_tag
      .flatMap(nd => Iterator((nd._1, nd._1), (nd._3, nd._3)))
      .cache()
    import sqlc.implicits._
    val vDF = vInitialRDD.distinct.toDF("id", "name").cache()
    import sqlc.implicits._

    vInitialRDD.unpersist(true)

    /*
     * we filter for edge type >= 0 because we use -1 edge type for isolated vertex
     * 1000 -1 1000 0 means 1000 is an isolated node
     */
    val multi_edges_TAG = findSimultaniousMultiEdges(initial_tag).cache()

    /*
     * Once simulatanious multi-edges are removed from the TAG, create GraphFrame
     */
    val eDF = multi_edges_TAG
      .filter(
        edge =>
          ((gETypes.contains(edge._2))
            || (edge._2 == -1)) //"isolated v"
      )
      .toDF("src", "type", "dst", "time")
      .cache()

    println("eDF size is ", eDF.count())
    // Create a GraphFrame
    import org.graphframes.GraphFrame
    var g = GraphFrame(vDF, eDF).cache()

    vDF.unpersist(true)

    val SYMMETRY = true
    val ASYMMETRY = false

    // Fix the isolated node calculation. exception in the file read create a -1 node which
    // is used by the code so there is 1 more isolated nodes than requried.
    g = findIsolatedVtx(g, "isolatednode", gETypes)
    g = findIsolatedEdg(g, "isolatededge", gETypes)
    g = findNonSimMultiEdg(g, "multiedge", gETypes)
    g = findSelfLoop(g, "selfloop", gETypes)
    g = findTriad(g, "triangle", SYMMETRY, gETypes).cache()
    g = findTriad(g, "triad", ASYMMETRY, gETypes).cache()
    g = findQuad(g, "twoloop", gETypes).cache()
    g = findQuad(g, "quad", gETypes)
    g = findDyad(g, "loop", SYMMETRY, gETypes, 2, 2).cache()
    g = findTriad(g, "outstar", SYMMETRY, gETypes).cache()
    g = findTriad(g, "instar", SYMMETRY, gETypes).cache()
    g = findDyad(g, "outdiad", SYMMETRY, gETypes, 3, 2).cache()
    g = findDyad(g, "indiad", SYMMETRY, gETypes, 3, 2).cache()
    g = findDyad(g, "inoutdiad", ASYMMETRY, gETypes, 3, 2).cache()
    g = findResidualEdg(g, "residualedge", gETypes).cache()

    if (gDebug) {
      println("FINAL after residual graph sizev ", g.vertices.count)
      println("graph size e", g.edges.count)
    }
    g

  }

  def complete_STM(
    gDebug: Boolean,
    gETypes: Array[Int],
    initial_simple_tag: SimpleTAGRDD
  ): (ListBuffer[Double], ListBuffer[Long]) = {
    var call_id = -1

    /*
     * Get total duration in seconds of input graph.
     */

    val allTimes = initial_simple_tag
      .filter(e => e._4 > -1)
      .flatMap(nd => Iterator(nd._4, nd._4))
    val minTime = allTimes.min
    val maxTime = allTimes.max
    val duration = maxTime - minTime
    val duration_days = (duration / 86400).toInt

    if (gDebug) {
      println("min time", minTime)
      println("max time", maxTime)
      println("duration in milliseconds", duration)
    }

    try {
      val g = findAllITeM(gETypes, call_id, initial_simple_tag)
      if (gDebug) {
        println(gMotifInfo.toList)
        println("number of edges in last graph", g.edges.count)
        println("number of vertex in last graph", g.vertices.count)
      }
    } catch {
      case e: Exception => {
        println("\nERROR: Call id = " + call_id)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())
      }
    }

    /*
     * Write current GMotifInfo to the "All" file
     */
    gMotifAllProbFWriter.println(
      1 + "," + 1 + "," + gMotifInfo.flatten.mkString(",")
    )
    gMotifAllProbFWriter
      .flush()
    gOffsetAllFWriter.println(
      1 + "," + 1 + "," + gOffsetInfo.flatten.mkString(",")
    )
    gMotifOrbitFWriter.println(
      1 + "," + 1 + "," + gMotifOrbitInfo.flatten.mkString(",")
    )
    gOffsetAllFWriter.flush()
    gMotifAllProb_IndividualFWriter.flush()
    gMotifOrbitFWriter.flush()

    /*
     * Generate Output
     * 1. Motif Probability
     * 2. Edge Offset Probability
     */
    val normMotifProb: ListBuffer[Double] =
      gMotifInfo.flatMap(f0 => f0.map(f1 => f1.toDouble / duration))
    gMotifProbFWriter.println(normMotifProb.mkString("\n"))
    //gMotifProbFile.println("duration in milliseconds=" + duration)

    val offsetProb: ListBuffer[Long] =
      gOffsetInfo.flatMap(f0 => f0.map(f1 => f1))
    gOffsetFWriter.println(offsetProb.mkString("\n"))

    /*
     * Output files
     */
    gMotifProbFWriter.flush()
    gOffsetFWriter.flush()

    return (normMotifProb, offsetProb)
  }

  /*
   * Get prefix annotation to name unique output files
   *
   */
  /*/
   * Define a dictionary of variable names and their meaning
   * Vtx : Vertex
   * Edg : Edge
   * Grf : Graph
   * Df  : DataFrame
   * Tmp : Temporal
   * Item: Independent Temporal Motifs
   *
   */

  def approx_STM(
    gDebug: Boolean,
    sampling_population: Int,
    sample_selection_prob: Double,
    num_iterations: Int,
    gETypes: Array[Int],
    initial_simple_tag: SimpleTAGRDD
  ): (ListBuffer[Double], ListBuffer[Long]) = {

    /*
     * Get total duration in seconds of input graph.
     */

    val allTimes = initial_simple_tag
      .filter(e => e._4 > -1)
      .flatMap(nd => Iterator(nd._4, nd._4))
    val minTime = allTimes.min
    val maxTime = allTimes.max
    val duration = maxTime - minTime

    if (gDebug) {
      println("min time", minTime)
      println("max time", maxTime)
      println("duration in milliseconds", duration)
    }

    val num_windows: Int = sampling_population
    val time_in_window: Long = duration / num_windows
    val total_edges: Long = initial_simple_tag.filter(e => e._4 > -1).count()
    var window_prob: ListBuffer[Double] = ListBuffer.empty

    for (i <- 0 to num_windows - 1) {
      val win_start_time = minTime + i * time_in_window
      val win_end_time = minTime + (i + 1) * time_in_window
      println("win start and end ", win_start_time, win_end_time)

      val edges_in_current_window: Long = initial_simple_tag
        .filter(
          e =>
            (e._4 > win_start_time) //start and end time does not include -1
              && (e._4 < win_end_time)
        )
        .count()
      println(" edges in current window i = ", i, edges_in_current_window)
      window_prob += edges_in_current_window.toDouble / total_edges
    }
    println("prob is " + window_prob)

    var gMotifInfo_global = ListBuffer.empty[Double]
    var gOffsetInfo_global = ListBuffer.empty[Long]

    for (itr <- 0 to num_iterations - 1) {
      currItrID = itr
      var gMotifInfo_itr_local = ListBuffer.empty[Double]
      var gOffsetInfo_itr_local = ListBuffer.empty[Long]
      var num_w_in_sampling = 0
      for (i <- 0 to num_windows - 1) {
        currWinID = i
        val rn = scala.util.Random
        if ((rn.nextDouble() < sample_selection_prob) || currWinID == 0) //forcing i==0 so that atleast one is picked
          {
            println(" i is " + i)
            gVertexIndependenceFWriter.println(
              "num_v_nonverlapping,num_v_max,v_independence_" + itr + "_" + i
            )
            gMotifIndependenceFWriter.println(
              "num_total_motif,num_ind_motif," +
                "motif_independence_" + itr + "_" + i
            )
            num_w_in_sampling = num_w_in_sampling + 1

            /*
            var local_g: GraphFrame = g
              .filterEdges(
                (col("time") > (minTime + i * time_in_window)) && (col("time") < (minTime + (i + 1) * time_in_window))
              )
              .dropIsolatedVertices()

            TODO : do something like this to inputsimpleTAG so that we reduce the size with each
            iteration.
            g = g
              .filterEdges(col("time") > (minTime + (i + 1) * time_in_window))
              .dropIsolatedVertices()
             */
            val local_tag = initial_simple_tag.filter(
              e =>
                (e._4 > (minTime + i * time_in_window)) &&
                  (e._4 < (minTime + (i + 1) *
                    time_in_window))
            )
            var call_id = 0
            try {
              findAllITeM(gETypes, call_id, local_tag)
            } catch {
              case e: Exception => {
                println("\nERROR: Call id = " + call_id)
                val sw = new StringWriter
                e.printStackTrace(new PrintWriter(sw))
                println("\n Exception is  " + sw.toString())
              }
            }

            /*
             * Write current GMotifInfo to the "All" file
             */
            gMotifAllProbFWriter.println(
              itr + "," + i + "," + gMotifInfo.flatten.mkString(",")
            )
            gMotifAllProbFWriter.flush()
            gOffsetAllFWriter.println(
              itr + "," + i + "," + gOffsetInfo.flatten.mkString(",")
            )
            gOffsetAllFWriter.flush()
            // gMotifInfo gOffsetInfo has counts for local graph
            if (gMotifInfo_itr_local.isEmpty) {
              //motif info is "rate of that motif devided by probability"
              // SO we need to first compute "rate" which is count/time_in_window
              gMotifInfo_itr_local = gMotifInfo.flatten
                .map(f1 => f1.toDouble / time_in_window)
                .map(m => (m / window_prob(i)))
              gOffsetInfo_itr_local =
                gOffsetInfo.flatten.map(m => (m / window_prob(i)).toLong)
              println(
                "gmotif info is empty " + gMotifInfo_itr_local.mkString("&&")
              )
            } else {
              //gMotifInfo is a list of list
              val weighted_motif_info = gMotifInfo.flatten
                .map(f1 => f1.toDouble / time_in_window)
                .map(m => m / window_prob(i))

              /*
               * We sum all the motif info and offset info coming from each window as per importnace sampling
               * definition. Outside of this loop, take averafe over all the sampled windows.
               */
              gMotifInfo_itr_local =
                gMotifInfo_itr_local.zip(weighted_motif_info).map {
                  case (x, y) => x + y
                }

              val weighted_offset_info =
                gOffsetInfo.flatten.map(m => (m / window_prob(i)).toLong)
              gOffsetInfo_itr_local =
                gOffsetInfo_itr_local.zip(weighted_offset_info).map {
                  case (x, y) => x + y
                }
              println(
                "gmotif info is empty " + gMotifInfo_itr_local.mkString("&&")
              )
            }
            gMotifInfo.clear
            gOffsetInfo.clear
            //both are reset becuase for next local graph motif computation, they should start with empty values
          }

      }
      gMotifInfo_itr_local =
        gMotifInfo_itr_local.map(m => m / num_w_in_sampling)
      gOffsetInfo_itr_local =
        gOffsetInfo_itr_local.map(o => o / num_w_in_sampling)

      /*
       * Add all local values of an iteration to the global list buffer to compute final answer
       */
      if (gMotifInfo_global.isEmpty)
        gMotifInfo_global = gMotifInfo_itr_local
      else
        gMotifInfo_global = gMotifInfo_global.zip(gMotifInfo_itr_local).map {
          case (x, y) => x + y
        }

      if (gOffsetInfo_global.isEmpty)
        gOffsetInfo_global = gOffsetInfo_itr_local
      else
        gOffsetInfo_global = gOffsetInfo_global.zip(gOffsetInfo_itr_local).map {
          case (x, y) => x + y
        }

    }

    /*
     * Average out global result for all the interations
     */
    gMotifInfo_global = gMotifInfo_global.map(m => m / num_iterations)
    gOffsetInfo_global = gOffsetInfo_global.map(o => o / num_iterations)
    gMotifProbFWriter.println(gMotifInfo_global.mkString("\n"))
    gOffsetFWriter.println(gOffsetInfo_global.mkString("\n"))
    /*
     * Output files
     */
    gMotifProbFWriter.flush()
    gOffsetFWriter.flush()
    (gMotifInfo_global, gOffsetInfo_global)
  }

  def findSimultaniousMultiEdges(inputSimpleTAG: SimpleTAGRDD): SimpleTAGRDD = {

    val sim_e =
      inputSimpleTAG
        .map(e => (e, 1))
        .reduceByKey((c1, c2) => c1 + c2)
        .filter(e => e._2 > 1)
        .cache()

    val sim_e_vpairs = sim_e.flatMap(e => Iterator(e._1._1, e._1._3)).distinct()
    val sim_e_num_v = sim_e_vpairs.count()
    val sim_e_max_num_v = (sim_e.values.sum() * 2).toLong
    /*
     * size is num_motif_nodes + 1. so use "0 to num_motif_nodes"
     *
     */
    val num_motif_nodes = 2
    val num_motif_edges = 1
    try {
      if (sim_e.isEmpty) {
        gMotifAllProb_IndividualFWriter.println(
          "sim_e",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1 }
        return inputSimpleTAG
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())
        gMotifAllProb_IndividualFWriter.println(
          "sim_e",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1 }
        return inputSimpleTAG

    }

    val reuse_node_info = sim_e
      .map(sim_inst => {
        val node_ids = List(sim_inst._1._1, sim_inst._1._3)
        // get times of all the edges
        val all_times = List(sim_inst._1._4)
        var numReusedNodes = 0

        var local_reuse_node_info: Map[Int, Int] = Map.empty
        for (i <- 0 to num_motif_nodes)
          local_reuse_node_info += (i -> 0)

        for (nid <- node_ids) {
          val node_time = gVBirthTime.getOrElse(nid, -1L)
          if (all_times.contains(node_time)) // new node so set its time as -1 for future
            {
              gVBirthTime(nid) = -1L
            } else
            numReusedNodes = numReusedNodes + 1

        }

        local_reuse_node_info = local_reuse_node_info +
          (numReusedNodes ->
            (local_reuse_node_info.getOrElse(numReusedNodes, 0) +
              (1 * sim_inst._2 - 1)))
        // multiply with actual number of such instances -1 because there is always 1 edge
        // which remains in the graph after duplicate edges are removed.
        local_reuse_node_info

      })
      .treeReduce((m1, m2) => {
        m1 |+| m2
      })

    val local_motif_info = reuse_node_info.values.toList
    gMotifAllProb_IndividualFWriter.println("sim_e", local_motif_info)
    gMotifInfo += local_motif_info
    println(gMotifInfo)

    write_vertex_independence(sim_e_num_v, sim_e_max_num_v)

    write_motif_vertex_association_file(sim_e.keys, "simulatanious")
    println("distict graph size is ", inputSimpleTAG.distinct().count())
    inputSimpleTAG.distinct()
  }
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  /**
    * get_vertex_arrival_time compute vertex birth time and return a RDD back
    *
    * @param nodeQuadruples : Edge of the TAG
    * @return
    */
  def get_vertex_birth_time(
    nodeQuadruples: RDD[(vertexId, eType, vertexId, eTime)]
  ): RDD[(Int, Long)] = {

    /*
     * compute first appearance time of each vertex.
     * (vid,etime)===> get smallest etime for a give vid
     */
    nodeQuadruples
      .flatMap(nd => Iterator((nd._1.toInt, nd._4), (nd._3.toInt, nd._4)))
      .reduceByKey((time1, time2) => Math.min(time1, time2))
  }

  def findNonSimMultiEdg(g: GraphFrame,
                         motifName: String,
                         gETypes: Array[Int]): GraphFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    var tmpG: GraphFrame = g.cache()
    for (et1 <- gETypes.indices) {
      for (et2 <- gETypes.indices) {
        if (gDebug) {
          println("multi edge graph sizev ", g.vertices.count)
          println("graph size e", g.edges.count)
        }

        //TODO: for a multi-edge type graph we need to think about removing the "multiedge"
        // because this is different than a multiedge in the graph with only one edge type.
        // There we can remove the smallest/largest edge. But if one edge is "call" and other is
        // "call" then which one to remove an WHY to remove is something to think about.
        val overlappingMotifs = tmpG
          .find(gAtomicMotifs(motifName))
          .filter("a != b")
          .filter("e1.type = " + gETypes(et1))
          .filter("e2.type = " + gETypes(et2))
          .filter("e1.time < e2.time")
        val selectEdgeArr = Array(
          "e1.src",
          "e1.type",
          "e1.dst",
          "e1.time",
          "e2.src",
          "e1.type",
          "e2.dst",
          "e2.time"
        )
        val selctedMotifEdges: DataFrame = overlappingMotifs
          .select(selectEdgeArr.head, selectEdgeArr.tail: _*)
          .cache()

        // https://stackoverflow.com/questions/32707620/how-to-check-if-spark-dataframe-is-empty
        try {
          if (selctedMotifEdges.head(1).isEmpty) {
            gMotifAllProb_IndividualFWriter.println("multi e", List(0))
            gMotifInfo += List(0)
            gOffsetInfo += List(-1L)
            return tmpG
          }
        } catch {
          case e: Exception =>
            val sw = new StringWriter
            e.printStackTrace(new PrintWriter(sw))
            println("\n Exception is  " + sw.toString())
            gMotifAllProb_IndividualFWriter.println("multi e", List(0))
            gMotifInfo += List(0)
            gOffsetInfo += List(-1L)
            return tmpG

        }
        /*
         * TODO : better way should be implemented than converting DF to RDD and then RDD to DF back
         * just to do flattening of edges.
         */
        val validMotifs =
          selctedMotifEdges.rdd.setName("validMotifMultiEdge").cache()
        println("overlapping multi-eddge ", validMotifs.count())
        /* get "representative edge" of the multi edges. for a given src,dst edge this is the
         * edge with lowest timestamp We create a set of edges for a given source-dst pair a
         * nd at the same time compute the smallest edge.  The first element i.e. the set gives
         * us the count of multi-edges and the 2nd element gives us the edges to keep.  Total
         * multi-edges are the sum of all 1st elements. We do -1 becuause for every edge set, there
         * is always 1 "edge to keep" and rest are the "multi edges"
         * For testGDifficult :
         * (3,(21,1,22,1001))
            (1,(3,1,0,1010))
            (1,(4,1,5,1011))
            (1,(1,1,4,1005))
            (1,(4,1,2,1006))
            (tatal multi edges are : ,7)
         *
         * multi_edges_info is src_dst_key, set of all mutli edges, minimum edge (representative
         * to keep)
         */
        val multi_edges_info: RDD[
          ((Int, Int), (Set[(Int, Int, Int, Long)], (Int, Int, Int, Long)))
        ] =
          validMotifs
            .flatMap(row => {

              val src_dst_key = (get_row_src(row), row.getAs[Int](2))

              val e1 = (
                get_row_src(row),
                row.getAs[Int](1),
                row.getAs[Int](2),
                row.getAs[Long](3)
              )
              val e2 = (
                row.getAs[Int](4),
                row.getAs[Int](5),
                row.getAs[Int](6),
                row.getAs[Long](7)
              )

              Iterator(
                (src_dst_key, (Set(e1), e1)),
                (src_dst_key, (Set(e2), e2))
              )
            })
            .reduceByKey((edge1, edge2) => {
              if (edge1._2._4 <= edge2._2._4) (edge1._1 ++ edge2._1, edge1._2)
              else (edge1._1 ++ edge2._1, edge2._2)
            })
            .cache()

        // unpersist validMotifs as it takes hume amount for some graphs Ex: wiki_talk
        validMotifs.unpersist(true)
        val avg_offset_time_perkey = multi_edges_info.map(m_info => {
          val all_multi_edges_on_srcdst = m_info._2._1
          val all_times = all_multi_edges_on_srcdst.map(me => me._4)
          val min_time = all_times.min
          val max_time = all_times.max
          (max_time - min_time).toDouble / all_multi_edges_on_srcdst.size
        })

        val avg_offset_time = avg_offset_time_perkey
          .sum() / avg_offset_time_perkey.count()
        gOffsetInfo += List(avg_offset_time.toLong)

        /*
         * (per key offset is ,Set((3,1,0,1010), (3,1,0,1013)),1.5)
          (per key offset is ,Set((4,1,2,1006), (4,1,2,1008)),1.0)
          (per key offset is ,Set((21,1,22,1002), (21,1,22,1055), (21,1,22,1004), (21,1,22,1001)),13.5)
          (per key offset is ,Set((4,1,5,1011), (4,1,5,1012)),0.5)
          (per key offset is ,Set((1,1,4,1005), (1,1,4,1007)),1.0)
         */
        val total_multi_edges =
          multi_edges_info.map(e => e._2._1.size - 1).sum().toInt
        val multi_edges_to_remove =
          multi_edges_info.flatMap(mi => mi._2._1 - mi._2._2)

        /*
         * write multi-edge nodes to a file
         */
        val multi_edge_nodes = multi_edges_to_remove
          .flatMap(me => {
            Iterable(me._1, me._3)
          })
          .distinct()
          .collect()
        val multi_edge_node_file = new PrintWriter(
          new File(
            t1 + "Motif_Vertex_Association_Multi_Edge" + prefix_annotation +
              "" +
              ".txt"
          )
        )
        multi_edge_nodes.foreach(v => multi_edge_node_file.println(v))
        multi_edge_node_file.flush()

        println("tatal multi edges are : ", total_multi_edges)
        println("avg avg_offset_time is ", avg_offset_time)
        val multi_edges_df = sqlc.createDataFrame(multi_edges_to_remove)

        // For reuse_node_info: For every motif, both the nodes are reused for the 2nd edge
        // So the resuling map it (2-> number of multi edges)
        val reuse_node_info: Map[Int, Int] = Map(2 -> total_multi_edges)
        gMotifAllProb_IndividualFWriter.println(
          "multi e",
          reuse_node_info.values.toList
        )
        gMotifInfo += reuse_node_info.values.toList
        println(" multi edge " + gMotifInfo)

        // Get time offset information
        /*
         * for a multi-edge motif, we need to remember only the temporal offset for the
         * 2nd edge. In the last step we have decided that the "representative edge" is one with
         * minimum timestamp, then the max edge is the "multi-edge" which is to be removed
         * and the motif distribution should only remember this. Because while generating
         * synthetic graph, we MUST generate only one multi-edge at a time where both the
         * src and dst are already generated
         *
         * filter("e1.time < e2.time") :
         * first edge is always smaller time than 2nd one so no need to check
         */

        val newEDF = tmpG.edges.except(multi_edges_df)
        import sqlc.implicits._
        val newVRDD = newEDF
          .flatMap(
            nd =>
              Iterator(
                (nd.getAs[Int](0), nd.getAs[Int](0)),
                (nd.getAs[Int](2), nd.getAs[Int](2))
            )
          )
          .distinct
          .toDF("id", "name")
        val newGraph = GraphFrame(newVRDD, newEDF)
        // unpersist old graph
        tmpG.unpersist(true)
        tmpG = newGraph.cache()
      }
    }
    tmpG
  }

  def findSelfLoop(g: GraphFrame,
                   motifName: String,
                   gETypes: Array[Int]): GraphFrame = {

    val spark = SparkSession.builder().getOrCreate()
    val sqlc = spark.sqlContext

    var tmpG: GraphFrame = g.cache()
    for (et1 <- gETypes.indices) {
      if (gDebug) {
        println("graph self loop sizev ", g.vertices.count)
        println("graph size e", g.edges.count)
      }
      val overlappingMotifs =
        tmpG
          .find(gAtomicMotifs(motifName))
          .filter("a == b")
          .filter("e1.type = " + gETypes(et1))
      val selectEdgeArr = Array("e1.src", "e1.type", "e1.dst", "e1.time")
      val selctedMotifEdges: DataFrame = overlappingMotifs
        .select(selectEdgeArr.head, selectEdgeArr.tail: _*)
        .distinct()
        .cache()

      val new_self_loop_cnt = selctedMotifEdges
        .filter(row => {
          val v = get_row_src(row)
          val t = get_row_time(row)
          gVBirthTime.getOrElse(v, -1) == t
        })
        .count()
        .toInt

      val total_self_loop_cnt = selctedMotifEdges.count()
      val reuse_self_loop_cnt =
        (total_self_loop_cnt - new_self_loop_cnt).toInt

      /*
       * write self-loop nodes to a file
       * write_motif_vertex_assosicate_file is not called becuase self loop does not have an RDD
       * of edge but only an rdd of self loop vertices
       */
      val self_loop_nodes = selctedMotifEdges.rdd
        .map(row => {
          get_row_src(row)
        })
        .distinct()
        .collect()

      writeMotifVertexAssoication(self_loop_nodes,motifName)

      val v_distinct = self_loop_nodes.length
      write_vertex_independence(v_distinct, total_self_loop_cnt)

      val newEDF = tmpG.edges.except(selctedMotifEdges)
      import sqlc.implicits._
      val newVRDD = newEDF
        .flatMap(
          nd =>
            Iterator(
              (nd.getAs[Int](0), nd.getAs[Int](0)),
              (nd.getAs[Int](2), nd.getAs[Int](2))
          )
        )
        .distinct
        .toDF("id", "name")
      val newGraph = GraphFrame(newVRDD, newEDF)
      gMotifAllProb_IndividualFWriter.println(
        "self loop",
        new_self_loop_cnt,
        reuse_self_loop_cnt
      )
      gMotifInfo += List(new_self_loop_cnt, reuse_self_loop_cnt)
      // unpersist old graph
      tmpG.unpersist(true)
      g.unpersist(true)
      tmpG = newGraph
      println("self loop done" + gMotifInfo)
      println("self loop done" + gOffsetInfo)
    }
    tmpG
  }

  /*
   * write a motif's nodes to a file
   */
  def write_motif_vertex_association_file(
    validMotifsArray: RDD[(Int, Int, Int, Long)],
    motifName:String
  ): Unit = {
    val multi_edge_nodes :Array[Int] = validMotifsArray
      .flatMap(e => {
        Iterator(e._1, e._3)
      })
      .distinct()
      .collect()

    writeMotifVertexAssoication(multi_edge_nodes,motifName)

  }

  def writeMotifVertexAssoication(allV: Array[Int], motifName: String): Unit = {
    gMotifVertexAssociationFWriter.println(
      currItrID + "," +
        currItrID + "," +
        gMotifNameToKey(motifName) + "," +
        allV.mkString(",")
    )
  }

  def findTriad(g: GraphFrame,
                motifName: String,
                symmetry: Boolean = false,
                gETypes: Array[Int]): GraphFrame = {

    println(
      "check if graph g v is chached " + g.vertices.storageLevel.useMemory
    )
    println("check if graph g e is chached " + g.edges.storageLevel.useMemory)

    var tmpG = g
    for (et1 <- gETypes.indices) {
      for (et2 <- gETypes.indices) {
        for (et3 <- gETypes.indices) {
          breakable {
            if (gDebug) {
              println("graph triad sizev ", g.vertices.count)
              println("graph size e", g.edges.count)
            }
            val validMotifsArray: RDD[(Int, Int, Int, Long)] =
              if (motifName.equalsIgnoreCase("outstar")
                  || motifName.equalsIgnoreCase("instar"))
                find3EdgNVtxMotifs(
                  tmpG,
                  motifName,
                  symmetry,
                  et1,
                  et2,
                  et3,
                  gETypes,
                  4,
                  3
                )
              else
                find3EdgNVtxMotifs(
                  tmpG,
                  motifName,
                  symmetry,
                  et1,
                  et2,
                  et3,
                  gETypes,
                  3,
                  3
                )
            if (validMotifsArray.isEmpty)
              break

            write_motif_vertex_association_file(validMotifsArray, motifName)
//            if (motifName.equalsIgnoreCase("outstar"))
//              write_motif_vertex_association_file(validMotifsArray, "outstar")
//            else if (motifName.equalsIgnoreCase("instar"))
//              write_motif_vertex_association_file(validMotifsArray, "instar")
//            else if (motifName.equalsIgnoreCase("triangle"))
//              write_motif_vertex_association_file(validMotifsArray, "triangle")
//            else if (motifName.equalsIgnoreCase("triad"))
//              write_motif_vertex_association_file(validMotifsArray, "triad")

            tmpG = get_new_graph_except_processed_motifs_edges(
              tmpG,
              validMotifsArray
            )
          }
        }
      }
    }
    tmpG
  }

  def get_new_graph_except_processed_motifs_edges(
    tmpG: GraphFrame,
    validMotifsArray: RDD[(Int, Int, Int, Long)]
  ): GraphFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val sqlc = spark.sqlContext

    val uniqeEDF = sqlc
      .createDataFrame(validMotifsArray)
      .toDF("src", "type", "dst", "time")

    /*
     * 			dataFrame's except methods returns distinct edges by default.
     *      I dont see the documentation saying this. I have fixed the graph reader code and do a "distinct" while
     *      creating the base RDD
     */
    val newEDF = tmpG.edges.except(uniqeEDF)
    import sqlc.implicits._
    val newVRDD = newEDF
      .flatMap(
        nd =>
          Iterator(
            (nd.getAs[Int](0), nd.getAs[Int](0)),
            (nd.getAs[Int](2), nd.getAs[Int](2))
        )
      )
      .distinct
      .toDF("id", "name")
    val newGraph = GraphFrame(newVRDD, newEDF)
    tmpG.unpersist(true)
    newGraph.cache()
  }

  /*
   * get edges from MIS motif found by the greedy MIS algo. MIS motif is just a concat string
   *
   */
  def get_edges_from_mis_motif(
    mis_set: RDD[String]
  ): RDD[(eType, eType, eType, eTime)] = {

    mis_set.flatMap(motif_id => {
      val mis_array_buff: ArrayBuffer[(Int, Int, Int, Long)] = ArrayBuffer.empty
      val all_motifs_edges = motif_id.split('|')
      all_motifs_edges.foreach(eid => {
        val eid_arr = eid.split("_")
        mis_array_buff.append(
          (
            eid_arr(0).toInt,
            eid_arr(1).toInt,
            eid_arr(2).toInt,
            eid_arr(3).toLong
          )
        )
      })
      mis_array_buff
    })
  }

  /*

   * construct motif from edges to compute their information content
   *
   * if node-time is < min-motif(t0) time ==> it is older node
   * if node-time is == t2 or t3 ==> it is a new node. IF it is new node, make sure to mark its time as -1 so that
   * no other edge in any other temporal motif can mark it a "new" again which leads to wrong expected number of
   * nodes.
   * else ==> it is older node (ie.e. between t0-t1 or t1-t2). node-time can not be more than t3
   * because node-time is minimum time of all the adjacent edges.
   */

  def get_node_reuse_info_from_mis_motif(
    num_motif_nodes: vertexId,
    num_motif_edges: vertexId,
    mis_set: RDD[String]
  ): Map[vertexId, vertexId] = {

    val gSC: SparkContext = SparkSession.builder.getOrCreate().sparkContext
    val gVBirthTime_exec = gSC.broadcast(gVBirthTime).value

    val reuse_node_info: Map[Int, Int] = mis_set
      .map(motifid => {

        //Get edge info
        val all_edges_ids: Array[String] = motifid.split('|')

        // array of all the edges
        val all_edges_arrs: ArrayBuffer[Array[String]] = ArrayBuffer.empty
        for (i <- 0 until num_motif_edges) {
          all_edges_arrs.append(all_edges_ids(i).split("_"))
        }

        // get times of all the edges
        val all_times = all_edges_arrs.map(e => e(3).toLong).toList
        var numReusedNodes = 0

        /*
         * Get Information content
         * var local_reuse_node_info : scala.collection.mutable.Map[Int,Int] = scala.collection
         * .mutable.Map.empty
         * immutable map is not supported by Scalaz
         *
         * size is num_motif_nodes + 1. so use "0 to num_motif_nodes"
         *
         */
        var local_reuse_node_info: Map[Int, Int] = Map.empty
        for (i <- 0 to num_motif_nodes)
          local_reuse_node_info += (i -> 0)

        /*
         * Get Node Ids
         *
         * For all the motifs for which |v| == |e| we can get source of each edge to
         * get the node ids
         *
         * Otherwise i.e. 2e3v, star creates a set of all the nodes and return as a list
         * The order is not preserved
         */
        val node_ids =
          if (num_motif_nodes == num_motif_edges)
            all_edges_arrs.map(e => e(0).toInt).toList
          else
            all_edges_arrs
              .flatMap(e => Set(e(0).toInt, e(2).toInt))
              .toSet
              .toList

        for (nid <- node_ids) {
          val node_time = gVBirthTime_exec.getOrElse(nid, -1L)
          if (all_times.contains(node_time)) // new node so set its time as -1 for future
            {
              /*
               * This is not a perfect solution to handling "new node" count because gVBirthTime
               * is computed from a braodcasted array so every executor has its own copy.
               * when we set -1L value, that is set only for that executor. So same node in another
               * executor will still be considered "new" because -1L value dose not propagate to that
               * executor.
               * This is still better than couting "new"  without broadcasting gVBirthTime
               * OR counting perfect number by collecting everything to driver
               *
               * ALSO : it check's node_time against all_times SO if some non-incidental edge has the
               * same time then also it will count it as a "new" node. Although the probability is
               * very less but still degrdes the quality of result
               *
               * TODO: improve "new" node approximation
               * It is overestimating "all reused" node motifs Ex:
               *  6.088413505562374E-9
             2.1309447269468312E-7
             1.6499600600074035E-6
             9.223946460926997E-6
             instead it has:
             0.0
             0.0
             0.0
             1.1062647339606835E-5
               */
              gVBirthTime_exec(nid) = -1L
            } else
            numReusedNodes = numReusedNodes + 1
        }

        local_reuse_node_info = local_reuse_node_info + (numReusedNodes
          -> (local_reuse_node_info.getOrElse(numReusedNodes, 0) + 1))
        local_reuse_node_info
      })
      .treeReduce((m1, m2) => {
        m1 |+| m2
      })
    reuse_node_info
  }

  def get_all_v_motif(
    row: Row,
    num_motif_edges: Int
  ): scala.collection.mutable.Set[Int] = {
    var set_of_v = scala.collection.mutable.Set[Int]()
    for (i <- 0 until num_motif_edges)
      set_of_v += (row.getAs[Int](i * 4), row.getAs[Int](i * 4 + 2))
    set_of_v
  }

  def writeOrbitIndependence_VertexAssociation(true_mis_set_rdd: RDD[String],
                                   num_nonoverlapping_m: Long,
                                   motifName: String): Unit = {
    /*
       "residualedge" -> "(a)-[e1]->(b)",: ALWAYS ONE SO DONT WRITE
       "selfloop" -> "(a)-[e1]->(b)",ALWAYS ONE SO DONT WRITE
       "multiedge" -> "(a)-[e1]->(b); (a)-[e2]->(b)",
       "isolatednode" -> "a", ALWAYS ONE SO DONT WRITE
       "isolatededge" -> "(a)-[e1]->(b)", ALWAYS (1,1) SO DONT WRITE
     */
    if (motifName.equalsIgnoreCase("triangle") ||
        motifName.equalsIgnoreCase("quad") ||
        motifName.equalsIgnoreCase("loop")) {
      // only one orbit
      val numVOrbit = get_edges_from_mis_motif(true_mis_set_rdd)
        .map(edge => edge._1)
        .distinct()
        .count
      gMotifOrbitInfo += List(numVOrbit.toDouble / num_nonoverlapping_m)
    } else if (motifName.equalsIgnoreCase("triad")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_vertex :RDD[(Int,Set[Int])]  = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          val e3 = edge_arr(2).split("_")
          Iterator((1, Set(e1(0).toInt)), (2, Set(e2(0).toInt)), (3, Set(e3(0).toInt)))
        })
        .distinct()

      val orbit_vertex_asso = orbit_vertex.reduceByKey((x, y) =>  x++y).collect

      /*
      * Write orbit_vertex_association file
       */
      val orbitIDMapMotif = gMotifNameToOrbitKeys(motifName)
      orbit_vertex_asso.foreach(ova => {
        gOrbitVertexAssociationFWriter.println(
          currItrID + "," + currWinID + "," +
            + orbitIDMapMotif(ova._1) + "," + ova._2.mkString(",")
        )
      })
      gOrbitVertexAssociationFWriter.flush()

      /*
       * Write orbit independence
       */
      val orbit_count: Map[Int, Int] = orbit_vertex_asso
      .map(x => (x._1, x._2.size))
        .toMap

      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(3, 0).toDouble / num_nonoverlapping_m
      )
      gMotifOrbitInfo += orbit_independence
    } else if (motifName.equalsIgnoreCase("outdiad") ||
               motifName.equalsIgnoreCase("indiad") ||
               motifName.equalsIgnoreCase("twoloop")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_count: Map[Int, Int] = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          //there are two orbits, a and b|c
          Iterator((1, e1(0).toInt), (2, e1(3).toInt), (2, e2(1).toInt))
        })
        .distinct()
        .map(x => (x._1, 1))
        .reduceByKey((x, y) => x + y)
        .collect()
        .toMap
      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m
      )
      gMotifOrbitInfo += orbit_independence
    } else if (motifName.equalsIgnoreCase("inoutdiad")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_count: Map[Int, Int] = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          //there are three orbits, a,b,c
          Iterator((1, e1(0).toInt), (2, e1(3).toInt), (3, e2(2).toInt))
        })
        .distinct()
        .map(x => (x._1, 1))
        .reduceByKey((x, y) => x + y)
        .collect()
        .toMap
      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(3, 0).toDouble / num_nonoverlapping_m
      )
      gMotifOrbitInfo += orbit_independence
    } else if (motifName.equalsIgnoreCase("instar") ||
               motifName.equalsIgnoreCase("outstar")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_count: Map[Int, Int] = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          val e3 = edge_arr(2).split("_")
          //there are two orbits, a and b|c|d
          Iterator(
            (1, e1(0).toInt),
            (2, e1(3).toInt),
            (2, e2(3).toInt),
            (2, e3(3).toInt)
          )
        })
        .distinct()
        .map(x => (x._1, 1))
        .reduceByKey((x, y) => x + y)
        .collect()
        .toMap
      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m
      )
      gMotifOrbitInfo += orbit_independence
    }
  }

  def find4EdgNVtxMotifs(
    tmpG: GraphFrame,
    motifName: String,
    et1: eType,
    et2: eType,
    et3: eType,
    et4: eType,
    gETypes: Array[eType],
    num_motif_nodes: Int,
    num_motif_edges: Int
  ): RDD[(vertexId, vertexId, vertexId, eTime)] = {

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    /*
     * Just having named vertices in the motif does not give unique results for each named vertex
     * For Ex: "quad" for the test graph returns
     * +---+----+---+----+
        |src|type|dst|time|
        +---+----+---+----+
        | 10|   1| 13|1009|
        | 13|   1| 10|1015|
        | 10|   1| 13|1009|
        | 13|   1| 10|1015|
        |  0|   1|  1|1000|
        |  1|   1|  2|1001|
        |  2|   1|  3|1002|
        |  3|   1|  0|1010|
        +---+----+---+----+

    Here 10,1,13,1009 and 13,1,10,1015 makes only one loop but are returned as quad because a,b,c,d
    are not treated uniquely. So we have to add this filter for every such query
     *
     */

    val overlappingMotifs: Dataset[Row] =
      if (num_motif_nodes == 3)
        tmpG
          .find(gAtomicMotifs(motifName))
          .filter(
            (col("a") =!= col("b")) &&
              (col("b") =!= col("c")) &&
              (col("a") =!= col("c"))
          )
          .filter("e1.type = " + gETypes(et1))
          .filter("e2.type = " + gETypes(et2))
          .filter("e3.type = " + gETypes(et3))
          .filter("e4.type = " + gETypes(et4))
      //.filter("e1.time < e2.time")
      //.filter("e2.time < e3.time")
      //.filter("e3.time < e4.time").cache()
      else
        tmpG
          .find(gAtomicMotifs(motifName))
          .filter(
            (col("a") =!= col("b")) &&
              (col("b") =!= col("c")) &&
              (col("c") =!= col("d")) &&
              (col("d") =!= col("a")) &&
              (col("a") =!= col("c")) &&
              (col("b") =!= col("d"))
          )
          .cache()
          .filter("e1.type = " + gETypes(et1))
          .filter("e2.type = " + gETypes(et2))
          .filter("e3.type = " + gETypes(et3))
          .filter("e4.type = " + gETypes(et4))
          .filter("e1.time < e2.time")
          .cache()
    //.filter("e2.time < e3.time")
    //.filter("e3.time < e4.time").cache()

    val selectEdgeArr = Array(
      "e1.src",
      "e1.type",
      "e1.dst",
      "e1.time",
      "e2.src",
      "e2.type",
      "e2.dst",
      "e2.time",
      "e3.src",
      "e3.type",
      "e3.dst",
      "e3.time",
      "e4.src",
      "e4.type",
      "e4.dst",
      "e4.time"
    )
    val selctedMotifEdges: DataFrame = overlappingMotifs
      .select(selectEdgeArr.head, selectEdgeArr.tail: _*)
      .cache()

    val num_overlapping_m = selctedMotifEdges.count()
    println("quad total non overlapping motifs are : ", num_overlapping_m)
    /*
     *selctedMotifEdges.show(100)
     * +---+----+---+----------+---+----+---+----------+---+----+---+----------+---+----+---+----------+
    |src|type|dst|      time|src|type|dst|      time|src|type|dst|      time|src|type|dst|      time|
    +---+----+---+----------+---+----+---+----------+---+----+---+----------+---+----+---+----------+
    |106|   0|111|1040583493|111|   0|492|1024692429|492|   0|316|1036385304|316|   0|106|1018642772|
    |106|   0|103|1012522993|103|   0|111|1042699512|111|   0|316|1015617449|316|   0|106|1018642772|
    |106|   0|111|1040583493|111|   0| 97|1024268131| 97|   0|316|1032832414|316|   0|106|1018642772|
    |106|   0|111|1040583493|111|   0|103|1024268020|103|   0|316|1050004092|316|   0|106|1018642772|
    |103|   0|111|1042699512|111|   0|316|1015617449|316|   0| 97|1015455157| 97|   0|103|1046662671|
    |103|   0|316|1050004092|316|   0|111|1018058640|111|   0| 97|1024268131| 97|   0|103|1046662671|
    |106|   0|103|1012522993|103|   0|316|1050004092|316|   0| 97|1015455157| 97|   0|106|1016843104|
    |106|   0|111|1040583493|111|   0|316|1015617449|316|   0| 97|1015455157| 97|   0|106|1016843104|
    |106|   0|103|1012522993|103|   0|111|1042699512|111|   0| 97|1024268131| 97|   0|106|1016843104|
    |316|   0|106|1018642772|106|   0|111|1040583493|111|   0| 97|1024268131| 97|   0|316|1032832414|
    |106|   0|103|1012522993|103|   0|316|1050004092|316|   0|111|1018058640|111|   0|106|1012749049|
    |111|   0| 97|1024268131| 97|   0|103|1046662671|103|   0|316|1050004092|316|   0|111|1018058640|
    |111|   0|106|1012749049|106|   0|103|1012522993|103|   0|316|1050004092|316|   0|111|1018058640|
    |316|   0|106|1018642772|106|   0|111|1040583493|111|   0|492|1024692429|492|   0|316|1036385304|
    |316|   0| 97|1015455157| 97|   0|111|1015207236|111|   0|492|1024692429|492|   0|316|1036385304|
    | 97|   0|111|1015207236|111|   0|103|1024268020|103|   0|316|1050004092|316|   0| 97|1015455157|
    | 97|   0|106|1016843104|106|   0|103|1012522993|103|   0|316|1050004092|316|   0| 97|1015455157|
     */
    val selctedMotifEdges_local_nonoverlap =
      get_local_NO_motifs(overlappingMotifs, selectEdgeArr, sqlc).cache()
    try {
      if (selctedMotifEdges_local_nonoverlap.head(1).isEmpty) {
        gMotifAllProb_IndividualFWriter.println(
          "4env ",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1 }
        return sc.emptyRDD
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())
        gMotifAllProb_IndividualFWriter.println(
          "4env ",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1 }
        return sc.emptyRDD

    }

    val valid_motif_overlap_graph = MaximumIndependentSet
      .getOverlapGraph(
        selctedMotifEdges_local_nonoverlap,
        sqlc,
        num_motif_nodes * num_motif_edges
      )
      .cache()
    if (gHigherGOut == true) {
      valid_motif_overlap_graph.vertices.collect
        .foreach(e => gHigherGraphFWriter.println(e.getAs[String](0)))
      gHigherGraphFWriter.flush()
      valid_motif_overlap_graph.edges.collect.foreach(
        e =>
          gHigherGraphFWriter
            .println(e.getAs[String](0) + "," + "" + e.getAs[String](1))
      )
      gHigherGraphFWriter.flush()
    }
    val mis_set: RDD[String] =
      MaximumIndependentSet.getMISGreedy(valid_motif_overlap_graph)

    val true_mis_set_rdd = get_local_NO_after_MIS(mis_set, sc).cache()
    valid_motif_overlap_graph.unpersist(true)

    val num_nonoverlapping_m = true_mis_set_rdd.count()

    writeOrbitIndependence_VertexAssociation(
      true_mis_set_rdd,
      num_nonoverlapping_m,
      motifName
    )
    val validMotifsArray: RDD[(Int, Int, Int, Long)] =
      get_edges_from_mis_motif(true_mis_set_rdd).cache()

    val reuse_node_info: Map[Int, Int] = get_node_reuse_info_from_mis_motif(
      num_motif_nodes,
      num_motif_edges,
      true_mis_set_rdd
    )

    gMotifAllProb_IndividualFWriter.println(
      "4env ",
      reuse_node_info.values.toList
    )
    gMotifInfo += reuse_node_info.values.toList

    write_motif_independence(num_overlapping_m, num_nonoverlapping_m)
    val v_distinct_cnt = get_v_distinct_cnt_from_true_mis_edges(
      validMotifsArray
    )
    write_vertex_independence(
      v_distinct_cnt,
      num_nonoverlapping_m * num_motif_nodes
    )

    println("quad " + gMotifInfo)
    println("quad time" + gOffsetInfo)
    // Get time offset infor
    val cnt_validMotifs = true_mis_set_rdd.count()
    val reuse_temporal_offset_info: ArrayBuffer[Long] =
      get_edge_time_offset_info_from_mis_motifs(
        num_motif_edges,
        true_mis_set_rdd
      )

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      reuse_temporal_offset_info.map(te => te / cnt_validMotifs)
    gOffsetInfo += avg_reuse_temporal_offset_info.toList

    validMotifsArray

  }

  def get_local_NO_after_MIS(mis_set: RDD[String],
                             gSC: SparkContext): RDD[String] = {
    /*
     * Again do a local filtering to make sure there is not overlapping motif selected even after
     * the getMISGreedy code. as shown in "share_by_one_motif.txt"
     */
    val mis_set_row_rdd: RDD[Row] = mis_set.map(m => {
      val edge_array = m.split("\\|")
      val motif_row: ArrayBuffer[String] = ArrayBuffer.empty
      edge_array.foreach(e => {
        val e_arr = e.split("_")
        motif_row += (e_arr(0), e_arr(1), e_arr(2), e_arr(3))
      })
      Row.fromSeq(motif_row)
    })
    val local_edge_set = scala.collection.mutable.HashSet[String]()

    val true_mis_set: Array[String] = mis_set_row_rdd
      .collect()
      .flatMap(motif_instnace => {
        val all_edges_motif =
          MaximumIndependentSet.getMotifEdges(motif_instnace)
        var edge_already_used = false
        all_edges_motif.map(e => {
          edge_already_used = edge_already_used || local_edge_set.contains(e)
        })
        if (!edge_already_used) {
          // None of the motif edge is already used so lets pick this motif and add all of its
          // edges to the local_edge_set
          local_edge_set ++= all_edges_motif
          Iterator(motif_instnace)
        } else Iterator.empty
      })
      .map(motif_instance => MaximumIndependentSet.getMotifId(motif_instance))

    val true_mis_set_rdd: RDD[String] = gSC.parallelize(true_mis_set)
    println("count of non overlaping motifs " + mis_set.count())
    println("count of TRUE non overlaping motifs " + true_mis_set_rdd.count())
    //true_mis_set_rdd.collect().foreach(m=>out_triad_nonoverlappinginstance_file.println(m))
    true_mis_set_rdd
  }

  def get_edge_time_offset_info_from_mis_motifs(
    num_motif_edges: Int,
    mis_set: RDD[String]
  ): ArrayBuffer[Long] = {
    val reuse_temporal_offset_info =
      mis_set
        .map(motifid => {

          val all_edges_ids = motifid.split('|')
          val all_edges_arrs: ArrayBuffer[Array[String]] = ArrayBuffer.empty

          // all_edges_arrs is an array(size of motif) of array(always size 4)
          for (i <- 0 until num_motif_edges) {
            all_edges_arrs.append(all_edges_ids(i).split("_"))
          }

          val local_reuse_temporal_offset_info: ArrayBuffer[Long] =
            ArrayBuffer.fill(num_motif_edges - 1)(0)

          /*
           * if we have 3 edges then we have 2 offset values. between e2e1, and e3e2
           *   OLD code // its should be size 2 map
       val t1 = e1(2).toLong
      val t2 = e2(2).toLong
      val t3 = e3(2).toLong
      //val t4 = e4(3).toLong
      local_reuse_temporal_offset_info(0) = t2 - t1
      local_reuse_temporal_offset_info(1) = t3 - t2
           */
          for (i <- 0 until num_motif_edges - 1) {
            local_reuse_temporal_offset_info(i) =
              all_edges_arrs(i + 1)(3).toLong - all_edges_arrs(i)(3).toLong

          }
          local_reuse_temporal_offset_info
        })
        .treeReduce((arr1, arr2) => {
          /*
           * Some arr1 has 3 offests (quad), some 2 (triag) some 1 (diad)
           */
          var res = ArrayBuffer[Long]()
          for (i <- 0 until arr1.length)
            res += ((arr1(i) + arr2(i)))
          //ArrayBuffer((arr1(0) + arr2(0)),
          //          (arr1(1) + arr2(1)))
          res
        })
    reuse_temporal_offset_info
  }

  def get_nonoverlapping_motif_inpartition(
    selctedMotifEdges: DataFrame
  ): RDD[Row] = {
    selctedMotifEdges.rdd.mapPartitionsWithIndex((partId, localrdd) => {
      // It is a local computation on an executor
      val local_edge_set = scala.collection.mutable.HashSet[String]()
      val local_indipendent_temporal_motifs: Iterator[Row] =
        localrdd.flatMap(motif_instnace => {
          val all_edges_motif =
            MaximumIndependentSet.getMotifEdges(motif_instnace)
          var edge_already_used = false
          all_edges_motif.map(e => {
            edge_already_used = edge_already_used || local_edge_set.contains(e)
          })
          if (edge_already_used == false) {
            // None of the motif edge is already used so lets pick this motif and add all of its
            // edges to the local_edge_set
            local_edge_set ++= all_edges_motif
            Iterator(motif_instnace)
          } else Iterator.empty
        })
      local_indipendent_temporal_motifs
    })
  }

  def get_v_distinct_cnt_from_true_mis_edges(
    validMotifsArray: RDD[(Int, Int, Int, Long)]
  ): Long = {
    validMotifsArray
      .flatMap(tmie => Iterator(tmie._1, tmie._3))
      .distinct()
      .count
  }

  def sneakyStar(
    tmpG: GraphFrame,
    num_motif_nodes: Int,
    num_motif_edges: Int,
    gSQLContext: SQLContext,
    motifName: String
  ): (Map[Int, Int], ArrayBuffer[Long], GraphFrame) = {
    val outheader = Array("id", "outDegree")
    val inheader = Array("id", "inDegree")
    val k = 10
    import org.apache.spark.sql.functions._
    val topKOut: Array[Row] =
      if (motifName.equalsIgnoreCase("outstar"))
        tmpG.outDegrees
          .select(outheader.head, outheader.tail: _*)
          .orderBy(desc("outDegree"))
          .limit(k)
          .rdd
          .collect()
      else
        tmpG.inDegrees
          .select(inheader.head, inheader.tail: _*)
          .orderBy(desc("inDegree"))
          .limit(k)
          .rdd
          .collect()

    val topK_V = topKOut.map(row => (row.getAs[Int](0)))
    println("top k are ", k, topK_V.toList)
    val in_out_star_edges =
      if (motifName.equalsIgnoreCase("outstar"))
        tmpG.find("(a)-[e1]->(b)").filter(col("a.id").isin(topK_V: _*))
      else tmpG.find("(a)-[e1]->(b)").filter(col("b.id").isin(topK_V: _*))
    val selectEdgeArr = Array("e1.src", "e1.type", "e1.dst", "e1.time")
    val selctedMotifEdges =
      in_out_star_edges.select(selectEdgeArr.head, selectEdgeArr.tail: _*)

    val high_star_edges =
      if (motifName.equalsIgnoreCase("outstar"))
        selctedMotifEdges.rdd
          .map(row => (get_row_src(row), row))
          .repartition(k)
          .cache()
      else
        selctedMotifEdges.rdd
          .map(row => (get_row_dst(row), row))
          .repartition(k)
          .cache()

    val global_high_star_motifs: RDD[scala.collection.mutable.Set[Row]] =
      high_star_edges
        .mapPartitionsWithIndex((partId, localrdd) => {
          val local_high_star_motifs =
            ArrayBuffer[scala.collection.mutable.Set[Row]]()
          // there should be one entry for each of the topk vertex
          val local_star_map = scala.collection.mutable
            .Map[Int, scala.collection.mutable.Set[Row]]()

          // this is a local computation
          localrdd.foreach(entry => {
            val vid = entry._1
            val e = entry._2
            val current_motif_edges =
              local_star_map.getOrElse(vid, scala.collection.mutable.Set[Row]())
            if (current_motif_edges.size == 2) {
              // the moment we get 3rd edge of the star, add that star to the local_high_star_motifs
              // and reset the map's entry for the topk vertex
              local_star_map(vid) = scala.collection.mutable.Set[Row]()
              val star = current_motif_edges += e
              local_high_star_motifs += star
            } else
              local_star_map(vid) = current_motif_edges += e
          })
          println(partId, local_star_map)
          local_high_star_motifs.toIterator
        })
        .repartition(400)
        .cache()

    // creating same datastruture to make it consistent with rest of the code
    val true_mis_set_rdd_star: RDD[String] =
      global_high_star_motifs.map(rowset => {
        val rowlist = rowset.toList
        //calling getEdgeID with 0 index becuase what we have is 3 different edges
        MaximumIndependentSet
          .getEdgeId(rowlist(0), 0) + "|" + MaximumIndependentSet
          .getEdgeId(rowlist(1), 0) + "|" + MaximumIndependentSet
          .getEdgeId(rowlist(2), 0)
      })

    if (true_mis_set_rdd_star.isEmpty()) {
      val default_motif_info: Map[Int, Int] =
        Map((0 -> 0), (1 -> 0), (2 -> 0), (3 -> 0))
      val defaul_timeoffset: ArrayBuffer[Long] = ArrayBuffer(-1L, -1L)
      return (default_motif_info, defaul_timeoffset, tmpG)

    }
    val cnt_validMotifs_star = true_mis_set_rdd_star.count
    val reuse_temporal_offset_info_star: ArrayBuffer[Long] =
      get_edge_time_offset_info_from_mis_motifs(
        num_motif_edges,
        true_mis_set_rdd_star
      )

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      reuse_temporal_offset_info_star.map(te => te / cnt_validMotifs_star)

    //true_mis_set_rdd_star.collect().foreach(s=>println(s))
    val validMotifsArray_star: RDD[(Int, Int, Int, Long)] =
      get_edges_from_mis_motif(true_mis_set_rdd_star).cache()

    val reuse_node_info_star = get_node_reuse_info_from_mis_motif(
      num_motif_nodes,
      num_motif_edges,
      true_mis_set_rdd_star
    )

    val filteredTmpG =
      get_new_graph_except_processed_motifs_edges(tmpG, validMotifsArray_star)
        .cache()
    (reuse_node_info_star, avg_reuse_temporal_offset_info, filteredTmpG)
  }

  /**
    * get_3eNv_motifs_mTypes function returns 3 vertex, 3 edges motif with multiple edge types
    *
    * @param g
    * @param motif
    * @param symmetry
    * @return
    */
  def find3EdgNVtxMotifs(tmpG: GraphFrame,
                         motifName: String,
                         symmetry: Boolean = false,
                         et1: eType,
                         et2: eType,
                         et3: eType,
                         gETypes: Array[Int],
                         num_motif_nodes: Int,
                         num_motif_edges: Int): RDD[(Int, Int, Int, Long)] = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    var reuse_node_info_star = Map[Int, Int]()
    for (i <- 0 to num_motif_nodes)
      reuse_node_info_star += (i -> 0)
    var avg_reuse_temporal_offset_info_star = ArrayBuffer[Long]()
    val overlappingMotifs =
      if (num_motif_nodes == 4) {

        println("original graph e ", tmpG.edges.count())
        val res =
          sneakyStar(tmpG, num_motif_nodes, num_motif_edges, sqlc, motifName)
        reuse_node_info_star = res._1
        avg_reuse_temporal_offset_info_star = res._2
        val newGraph = res._3.cache()
        println("Sneaky graph e ", newGraph.edges.count())
        newGraph
          .find(gAtomicMotifs(motifName))
          .filter("a != b")
          .filter("b != c")
          .filter("c != a")
          .filter("a != d")
          .filter("e1.type = " + gETypes(et1))
          .filter("e2.type = " + gETypes(et2))
          .filter("e3.type = " + gETypes(et3))
          .filter("a.id < c.id")
          // reducing candidate tow loop i.e. azc or cza=> pick only azc
          // time based restriction wont work for this motif type
          //.filter("e1.time < e2.time")
          //.filter("e2.time < e3.time")
          .cache()

      } else if (symmetry)
        tmpG
          .find(gAtomicMotifs(motifName))
          .filter("a != b")
          .filter("b != c")
          .filter("c != a")
          .filter("e1.type = " + gETypes(et1))
          .filter("e2.type = " + gETypes(et2))
          .filter("e3.type = " + gETypes(et3))
          .filter("e1.time < e2.time")
          .cache()
      //.filter("e2.time < e3.time").cache()
      else
        tmpG
          .find(gAtomicMotifs(motifName))
          .filter("a != b")
          .filter("b != c")
          .filter("c != a")
          .filter("e1.type = " + gETypes(et1))
          .filter("e2.type = " + gETypes(et2))
          .filter("e3.type = " + gETypes(et3))
          .cache()
    val selectEdgeArr = Array(
      "e1.src",
      "e1.type",
      "e1.dst",
      "e1.time",
      "e2.src",
      "e2.type",
      "e2.dst",
      "e2.time",
      "e3.src",
      "e3.type",
      "e3.dst",
      "e3.time"
    )

    val num_overlap_motifs = overlappingMotifs.count()
    println("num_overlap_motifs count is", num_overlap_motifs)
    val selctedMotifEdges_local_nonoverlap =
      get_local_NO_motifs(overlappingMotifs, selectEdgeArr, sqlc).cache()

    // get unique motif
    try {
      if (selctedMotifEdges_local_nonoverlap.head(1).isEmpty) {
        gMotifAllProb_IndividualFWriter.println(
          "3env",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1L }
        return sc.emptyRDD
      }
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())
        gMotifAllProb_IndividualFWriter.println(
          "3env",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1L }
        return sc.emptyRDD
      }
    }

    val valid_motif_overlap_graph = MaximumIndependentSet
      .getOverlapGraph(
        selctedMotifEdges_local_nonoverlap,
        sqlc,
        num_motif_nodes * num_motif_edges
      )
      .cache()
    if (gHigherGOut == true) {
      valid_motif_overlap_graph.vertices.collect
        .foreach(e => gHigherGraphFWriter.println(e.getAs[String](0)))
      gHigherGraphFWriter.flush()
      valid_motif_overlap_graph.edges.collect.foreach(
        e =>
          gHigherGraphFWriter
            .println(e.getAs[String](0) + "," + "" + e.getAs[String](1))
      )
      gHigherGraphFWriter.flush()
    }
    val mis_set: RDD[String] =
      MaximumIndependentSet.getMISGreedy(valid_motif_overlap_graph)
    val true_mis_set_rdd: RDD[String] =
      get_local_NO_after_MIS(mis_set, sc).cache()

    valid_motif_overlap_graph.unpersist(true)

    val num_nonoverlap_motifs = true_mis_set_rdd.count()
    writeOrbitIndependence_VertexAssociation(
      true_mis_set_rdd,
      num_nonoverlap_motifs,
      motifName
    )

    val validMotifsArray: RDD[(Int, Int, Int, Long)] =
      get_edges_from_mis_motif(true_mis_set_rdd).cache()

    val v_distinct_cnt = get_v_distinct_cnt_from_true_mis_edges(
      validMotifsArray
    )
    /* mis_set is:
     * 1612_0_588_1355184824|588_0_589_1357159776|1612_0_589_1355764972,
     * 1612_0_588_1355184824|588_0_589_1357159776|1612_0_589_1357153244
     */

    /*
     * It is possible that multi-edge nodes are never counted for "newnode". because we set all
     * mutliedge nodes as "reused" in the code of mutli-edge. if the same node is used in another
     * motif later, it is again found to be an "old" node so that node is no-where counted as
     * "fresh" node. Ex in testGDifficult
     * 0,0,1,0  : for triad where node 5 is used "reused" in mutliedge and triad both. should fix
     * it in multi edge code. NOT a major issue though.
     */
    val reuse_node_info: Map[Int, Int] = get_node_reuse_info_from_mis_motif(
      num_motif_nodes,
      num_motif_edges,
      true_mis_set_rdd
    )

    println("sneaky star gMotif ", reuse_node_info_star.values.toList)
    println("Non sneaky star gMotif ", reuse_node_info.values.toList)

    val local_res = (reuse_node_info_star.values.toList
      .zip(reuse_node_info.values.toList))
      .map {
        case (x, y) => x + y
      }
    gMotifAllProb_IndividualFWriter.println("3env ", local_res)
    gMotifInfo += local_res

    write_motif_independence(num_overlap_motifs, num_nonoverlap_motifs)
    write_vertex_independence(
      v_distinct_cnt,
      num_nonoverlap_motifs * num_motif_nodes
    )
    // Get time offset information
    val cnt_validMotifs = true_mis_set_rdd.count()

    val reuse_temporal_offset_info: ArrayBuffer[Long] =
      get_edge_time_offset_info_from_mis_motifs(
        num_motif_edges,
        true_mis_set_rdd
      )

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      reuse_temporal_offset_info.map(te => te / cnt_validMotifs)
    //TODO: THIS way of adding time offset does not seems correct..
    gOffsetInfo += (avg_reuse_temporal_offset_info.toList
      .zip(avg_reuse_temporal_offset_info.toList))
      .map { case (x, y) => x + y }

    validMotifsArray
  }

  def get_local_NO_motifs(overlappingMotifs: Dataset[Row],
                          selectEdgeArr: Array[String],
                          gSQLContext: SQLContext): DataFrame = {
    val selctedMotifEdges = overlappingMotifs
      .select(selectEdgeArr.head, selectEdgeArr.tail: _*)
      .persist(StorageLevel.MEMORY_ONLY)

    val selctedMotifEdges_NonOverRDD: RDD[Row] =
      get_nonoverlapping_motif_inpartition(selctedMotifEdges).cache()

    try {
      println(
        " selctedMotifEdges_NonOverRDD count is " + selctedMotifEdges_NonOverRDD
          .count()
      )
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())

      }
    }
    val motif_schme = selctedMotifEdges.schema

    selctedMotifEdges.unpersist(true)
    import gSQLContext.implicits._
    gSQLContext.createDataFrame(selctedMotifEdges_NonOverRDD, motif_schme)

  }

  def find2EdgNVtxMotifs(tmpG: GraphFrame,
                         motifName: String,
                         symmetry: Boolean = false,
                         et1: eType,
                         et2: eType,
                         gETypes: Array[Int],
                         num_motif_nodes: Int,
                         num_motif_edges: Int): RDD[(Int, Int, Int, Long)] = {
    println(" Staring 2e3v motif nV, vE", num_motif_nodes, num_motif_edges)
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    val overlappingMotifs =
      if (num_motif_nodes == 2) {
        if (symmetry)
          tmpG
            .find(gAtomicMotifs(motifName))
            .filter("a != b")
            .filter("e1.type = " + gETypes(et1))
            .filter("e2.type = " + gETypes(et2))
            .filter("e1.time < e2.time")
            .cache()
        else
          tmpG
            .find(gAtomicMotifs(motifName))
            .filter("a != b")
            .filter("e1.type = " + gETypes(et1))
            .filter("e2.type = " + gETypes(et2))
            .cache()

      } else {
        if (symmetry)
          tmpG
            .find(gAtomicMotifs(motifName))
            .filter("a != b")
            .filter("b != c")
            .filter("c != a")
            .filter("e1.type = " + gETypes(et1))
            .filter("e2.type = " + gETypes(et2))
            .filter("e1.time < e2.time")
            .cache()
        else
          tmpG
            .find(gAtomicMotifs(motifName))
            .filter("a != b")
            .filter("b != c")
            .filter("c != a")
            .filter("e1.type = " + gETypes(et1))
            .filter("e2.type = " + gETypes(et2))
            .cache()
      }
    val selectEdgeArr = Array(
      "e1.src",
      "e1.type",
      "e1.dst",
      "e1.time",
      "e2.src",
      "e1.type",
      "e2.dst",
      "e2.time"
    )
    val num_overlapping_m = overlappingMotifs.count()
    val selctedMotifEdges_local_nonoverlap: DataFrame =
      get_local_NO_motifs(overlappingMotifs, selectEdgeArr, sqlc).cache()
    try {
      if (selctedMotifEdges_local_nonoverlap.head(1).isEmpty) {
        gMotifAllProb_IndividualFWriter.println(
          "2env",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1 }
        return sc.emptyRDD
      }
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())
        gMotifAllProb_IndividualFWriter.println(
          "2env",
          List.fill(num_motif_nodes + 1) { 0 }
        )
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(num_motif_edges - 1) { -1 }
        return sc.emptyRDD
      }
    }

    val valid_motif_overlap_graph = MaximumIndependentSet
      .getOverlapGraph(
        selctedMotifEdges_local_nonoverlap,
        sqlc,
        num_motif_nodes * num_motif_edges
      )
      .cache()
    if (gHigherGOut == true) {
      valid_motif_overlap_graph.vertices.collect
        .foreach(e => gHigherGraphFWriter.println(e.getAs[String](0)))
      gHigherGraphFWriter.flush()
      valid_motif_overlap_graph.edges.collect.foreach(
        e =>
          gHigherGraphFWriter
            .println(e.getAs[String](0) + "," + "" + e.getAs[String](1))
      )
      gHigherGraphFWriter.flush()
    }
    println("overlapping graph e", valid_motif_overlap_graph.edges.count)
    println("overlapping graph v", valid_motif_overlap_graph.vertices.count)
    val mis_set: RDD[String] =
      MaximumIndependentSet.getMISGreedy(valid_motif_overlap_graph).cache()
    /* mis-set is:
     * 1612_588_1355184824|588_589_1357159776|1612_589_1355764972,1612_588_1355184824|588_589_1357159776|1612_589_1357153244
     */
    val true_mis_set_rdd = get_local_NO_after_MIS(mis_set, sc).cache()
    val num_nonoverlapping_m = true_mis_set_rdd.count()
    writeOrbitIndependence_VertexAssociation(
      true_mis_set_rdd,
      num_nonoverlapping_m,
      motifName
    )

    val validMotifsArray: RDD[(Int, Int, Int, Long)] = get_edges_from_mis_motif(
      true_mis_set_rdd
    )

    valid_motif_overlap_graph.unpersist(true)
    println("2e 3v mis set size is " + mis_set.count())
    println("2e 3v mis set size is " + true_mis_set_rdd.count())

    /*
     * construct motif from edges to compute their information content
     *
     */
    val reuse_node_info: Map[Int, Int] =
      get_node_reuse_info_from_mis_motif(
        num_motif_nodes,
        num_motif_edges,
        true_mis_set_rdd
      )

    gMotifAllProb_IndividualFWriter.println(
      "2env",
      reuse_node_info.values.toList
    )
    gMotifInfo += reuse_node_info.values.toList

    write_motif_independence(num_overlapping_m, num_nonoverlapping_m)
    val v_distinct_cnt = get_v_distinct_cnt_from_true_mis_edges(
      validMotifsArray
    )
    write_vertex_independence(
      v_distinct_cnt,
      num_nonoverlapping_m * num_motif_nodes
    )

    // Get time offset infor
    val cnt_validMotifs = true_mis_set_rdd.count()
    val reuse_temporal_offset_info: ArrayBuffer[Long] =
      get_edge_time_offset_info_from_mis_motifs(
        num_motif_edges,
        true_mis_set_rdd
      )

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      reuse_temporal_offset_info.map(te => te / cnt_validMotifs)
    gOffsetInfo += avg_reuse_temporal_offset_info.toList

    true_mis_set_rdd.unpersist(true)
    println(" Finishing 2e3v motif" + gMotifInfo)
    println(" Finishing 2e3v timeoffset" + gOffsetInfo)
    validMotifsArray
  }

  def findDyad(g: GraphFrame,
               motifName: String,
               symmetry: Boolean = false,
               gETypes: Array[Int],
               num_motif_nodes: Int,
               num_motif_edges: Int): GraphFrame = {
    var tmpG = g
    for (et1 <- gETypes.indices) {
      for (et2 <- gETypes.indices) {
        val spark = SparkSession.builder().getOrCreate()
        val sc = spark.sparkContext
        val sqlc = spark.sqlContext

        if (gDebug) {
          println("graph dyad sizev ", g.vertices.count)
          println("graph size e", g.edges.count)
        }
        val validMotifsArray = find2EdgNVtxMotifs(
          tmpG,
          motifName,
          symmetry,
          et1,
          et2,
          gETypes,
          num_motif_nodes,
          num_motif_edges
        ).cache()
        write_motif_vertex_association_file(validMotifsArray,motifName)

        val uniqeEDF = sqlc
          .createDataFrame(validMotifsArray)
          .toDF("src", "type", "dst", "time")

        /*
         * 			dataFrame's except methods returns distinct edges by default.
         * 			See more detail in processUniqueMotif_3Edges method
         *
         */
        val newEDF = tmpG.edges.except(uniqeEDF)
        import sqlc.implicits._
        val newVRDD = newEDF
          .flatMap(
            nd =>
              Iterator(
                (nd.getAs[Int](0), nd.getAs[Int](0)),
                (nd.getAs[Int](2), nd.getAs[Int](2))
            )
          )
          .distinct
          .toDF("id", "name")
        import sqlc.implicits._
        val newGraph = GraphFrame(newVRDD, newEDF)
        tmpG = newGraph
      }
    }

    tmpG
  }

  def get_row_src(row: Row): Int = {
    row.getAs[Int](0)
  }

  def get_row_etype(row: Row): Int = {
    row.getAs[Int](1)
  }

  def get_row_dst(row: Row): Int = {
    row.getAs[Int](2)
  }

  def get_row_time(row: Row): Long = {
    row.getAs[Long](3)
  }

  /**
    * look for 1-edge motif a->b for all possible edge types
    *
    * @param g
    * @param motif
    * @return
    */
  def findResidualEdg(g: GraphFrame,
                      motifName: String,
                      gETypes: Array[Int]): GraphFrame = {

    if (gDebug) {
      println("graph sizev ", g.vertices.count)
      println("graph size e", g.edges.count)
    }

    val nodeReuse: ArrayBuffer[Int] = ArrayBuffer.fill(3)(0)
    var tmpG = g
    for (et1 <- gETypes.indices) {
      val overlappingMotifs =
        tmpG
          .find(gAtomicMotifs(motifName))
          .filter("a != b")
          .filter("e1.type = " + gETypes(et1))
      val selectEdgeArr = Array("e1.src", "e1.type", "e1.dst", "e1.time")
      val selctedMotifEdges = overlappingMotifs
        .select(selectEdgeArr.head, selectEdgeArr.tail: _*)
        .cache()

      val num_residual_edges = selctedMotifEdges.count()
      try {
        if (selctedMotifEdges.head(1).isEmpty) {
          gMotifAllProb_IndividualFWriter.println("redi e", List(0))
          gMotifInfo += List(0)
          //gOffsetInfo += List(0L)
          return tmpG
        }
      } catch {
        case e: Exception => {
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          println("\n Exception is  " + sw.toString())
          gMotifAllProb_IndividualFWriter.println("redi e", List(0))
          gMotifInfo += List(0)
          //gOffsetInfo += List(0L)
          return tmpG
        }
      }

      /*
       * write residual nodes to a file
       */
      val resi_edge_nodes = selctedMotifEdges.rdd
        .flatMap(row => {
          Iterable(get_row_src(row), get_row_dst(row))
        })
        .distinct()
        .collect()
      val motif_v_file = new PrintWriter(
        new File(
          t1 + "residual" + prefix_annotation +
            "" +
            ".txt"
        )
      )
      resi_edge_nodes.foreach((v => motif_v_file.println(v)))
      motif_v_file.flush()

      /*
       * For residual edges, there can be one "new node"
       * Ex for input graph G =
       * 1,1,2,1001
       * 2,1,3,1002
       * 3,1,4,1003
       *
       * we have 1 wedge and 1 residual edge. Ex: 1->2->3 + 3->4
       * for the residual edge, 4 is a "new node" and 3 is reused
       *
       * Both the nodes can not be "new" because that is a "isolated edge" by definition and must
       * be identified earlier.
       */
      val one_new_nodes_motif_cnt = selctedMotifEdges
        .filter(row => {
          val src = get_row_src(row)
          val dst = get_row_dst(row)
          val etime = get_row_time(row)
          if ((gVBirthTime.getOrElse(src, -1) == etime) || (gVBirthTime
                .getOrElse(dst, -1) == etime))
            true
          else false
        })
        .count()
        .toInt

      write_motif_independence(num_residual_edges, num_residual_edges)
      write_motif_independence(num_residual_edges * 2, num_residual_edges * 2)
      // total number of nodes in residual edges are 2*number of edges because IF NOT they are
      // not residual edge but a wedge
      val reused_node_cnt =
        (num_residual_edges - one_new_nodes_motif_cnt).toInt
      gMotifAllProb_IndividualFWriter.println(
        List(one_new_nodes_motif_cnt, reused_node_cnt)
      )
      gMotifInfo += List(one_new_nodes_motif_cnt, reused_node_cnt)
    }
    tmpG
  }

//  val out_motif_instance_file = new PrintWriter(
//    (new File("out_motif_instance.txt"))
//  )
  def moveFilesToOutdir(output_base_dir: String): Unit = {
    /*
     * Move all files to output dir
     */
    def moveFileInner(srcFileObj: File): Unit = {
      import java.nio.file.Files
      Files.move(
        Paths.get(srcFileObj.getAbsolutePath),
        Paths.get(output_base_dir + "/" + srcFileObj.getName)
      )
    }
    val out_dir_base = new File(output_base_dir)
    if (!out_dir_base.exists())
      out_dir_base.mkdirs()

    try {
      moveFileInner(gMotifProbFile)
      moveFileInner(gMotifAllProbFile)
      moveFileInner(gMotifAllProbFile_Individual)
      moveFileInner(gOffsetFile)
      moveFileInner(gOffsetAllFile)
      moveFileInner(gVertexBirthFile)
      moveFileInner(gMotifIndependenceFile)
      moveFileInner(gVertexIndependenceFile)
      moveFileInner(gHigherGraphFile)

      val directory = new File(".")
      println("curr dir is ", directory.getAbsolutePath)
      val pattern = "^.*" + t1 + ".*.txt$"
      System.out.println("\nFiles that match regular expression: " + pattern)
      val filter: FileFilter = new RegexFileFilter("^.*" + t1 + ".*.txt$")
      val files = directory.listFiles(filter)

      println("Files to move are ", files.toList)
      files.foreach(afile => moveFileInner(afile))
    } catch {
      case e: Exception => {
        println("\nERROR: Failed to move files = ")
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        println("\n Exception is  " + sw.toString())
      }
    }

  }

  def write_motif_independence(overlapping_cnt: Long,
                               non_overlapping_cnt: Long): Unit = {
    // write motif uniqueness file
    gMotifIndependenceFWriter.println(
      overlapping_cnt + "," +
        non_overlapping_cnt + "," +
        non_overlapping_cnt.toDouble / overlapping_cnt.toDouble
    )
    gMotifIndependenceFWriter.flush()
  }

  def write_vertex_independence(num_v_nonoverlapping: Long,
                                num_v_max_possible: Long) = {
    gVertexIndependenceFWriter.println(
      num_v_nonoverlapping + "," + num_v_max_possible + "," +
        num_v_nonoverlapping.toDouble / num_v_max_possible.toDouble
    )
    gVertexIndependenceFWriter.flush()
  }
}
