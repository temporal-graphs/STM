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
import gov.pnnl.builders.TAGBuilder
import gov.pnnl.datamodel.GlobalTypes._
import gov.pnnl.stm.conf.STMConf
import gov.pnnl.stm.datamodel.timeOffset
import org.apache.commons.io.filefilter.RegexFileFilter
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import gov.pnnl.datamodel.TAG
import gov.pnnl.stm.algorithms.STM_NodeArrivalRateMultiType.{gVertex_ITeM_Freq, gVertex_Orbit_Freq}
import org.apache.spark.graphx.VertexId

/**
  * @author puro755
  *
  */
object STM_NodeArrivalRateMultiType {

  /*
   * Many global variables are define at the end of the file
   * FileWriter are abbrevated as FWr
   */

  println("######OBJECT CREATED ; STM_NodeArrivalRateMultiType ######")
  val t1 = System.nanoTime()
  val prefix_annotation = ""
  val gGraphEmebdding = ListBuffer[Object]()
  val gMotifInfo = ListBuffer.empty[List[Int]]
  val gOrbit_Ind = ListBuffer.empty[List[Double]]
  val gOffsetInfo = ListBuffer.empty[List[Long]]
  var gVertex_ITeM_Freq : Map[Int,Map[Int,Int]] = scala.collection.Map.empty
  var gVertex_Orbit_Freq : Map[Int,Map[Int,Int]] = scala.collection.Map.empty

  //ALL THESE FILES ARE GETTING CREATED IN EACH EXECUTOR ALSO
  val gITeMRateFile = new File(
    t1 + "_ITeM_RateAvg" + prefix_annotation + ".txt"
  )
  val gITeMRateFWr = new PrintWriter(gITeMRateFile)
  val gITeM_FreqFile = new File(
    t1 + "_ITeM_Freq" + prefix_annotation + ".txt"
  )
  val gITeM_FreqFWr = new PrintWriter(gITeM_FreqFile)
  gITeM_FreqFWr.println("[")

  /*
  * Orbit Independence for each oribit position
   */
  val gOrbitFile = new File(
    t1 + "_Orbit_Ind" + prefix_annotation + ".txt"
  )
  val gOrbit_Ind_FWr = new PrintWriter( gOrbitFile )

  val gOffsetFile = new File(t1 + "_Offset_RateAvg" + prefix_annotation + ".txt")
  val gOffsetFWriter = new PrintWriter(gOffsetFile)
  val gOffsetAllFile = new File(
    t1 + "_Offset_AbsCount" + prefix_annotation + ".txt"
  )
  val gOffsetAllFWriter = new PrintWriter(gOffsetAllFile)
  gOffsetAllFWriter.println("[")
  val gVertexBirthFile = new File(
    t1 + "_VertexBirth" + prefix_annotation + ".txt"
  )
  val gVertexBirthFWriter = new PrintWriter(gVertexBirthFile)
  val gITeM_IndFile = new File(
    t1 + "_Motif_Ind" + prefix_annotation + ".txt"
  )
  val gITeM_IndFWr = new PrintWriter(
    new FileWriter(gITeM_IndFile, true)
  )
  val gVtxIndFile = new File(
    t1 + "_Vertex_Ind" + prefix_annotation + ".txt"
  )
  val gVtxIndFWr = new PrintWriter(
    new FileWriter(gVtxIndFile, true)
  )
  val gOrbtVtxAssoFile = new File(
    t1 + "_Orbit_Association" + prefix_annotation + ".txt"
  )
  val gOrbtVtxAssoFWr = new PrintWriter(
    new FileWriter(gOrbtVtxAssoFile, true)
  )
  val gMotifVtxCooccurFile = new File(
    t1 + "_Motif_Cooccurance" + prefix_annotation + ".txt"
  )
  val gMotifVtxCooccurFWr = new PrintWriter(
    new FileWriter(gMotifVtxCooccurFile, true)
  )
  val gMotifVtxAssoFile = new File(
    t1 + "_Motif_Association" + prefix_annotation + ".txt"
  )
  val gMotifVtxAssoFWr = new PrintWriter(
    new FileWriter(gMotifVtxAssoFile, true)
  )
  val gVexITeMFreqFile = new File(
    t1 + "_Vertex_ITeM_Frequency" + prefix_annotation + ".txt"
  )
  val gVertexITeMFreqFWr = new PrintWriter(
    new FileWriter(gVexITeMFreqFile, true)
  )
  val gVexOrbitFreqFile = new File(
    t1 + "_Vertex_Orbit_Frequency" + prefix_annotation + ".txt"
  )
  val gVertexOrbitFreqFWr = new PrintWriter(
    new FileWriter(gVexOrbitFreqFile, true)
  )
  val gHigherGraphFile = new File(
    t1 + "_HigherGraph" + prefix_annotation + "" + ".txt"
  )
  val gHigherGraphFWriter = new PrintWriter(
    new FileWriter(gHigherGraphFile, true)
  )


  val gWindowTimeFile = new File(
                                   t1 + "_WindowTime" + prefix_annotation + "" + ".txt"
                                 )
  val gWindowTimeFWriter = new PrintWriter(
                                             new FileWriter(gWindowTimeFile, true)
                                           )

  val gWindowSizeFile = new File(
                                  t1 + "_WindowSize" + prefix_annotation + "" + ".txt"
                                )
  val gWindowSizeFWriter = new PrintWriter(
                                            new FileWriter(gWindowSizeFile, true)
                                          )
  val nodemapFileObj = new File(t1 + "_nodeMap.txt")
  val nodemapFile = new PrintWriter(nodemapFileObj)

  val gDebug = false
  val gHigherGOut = true
  val gAtomicMotifs: Map[String, String] = STMConf.atomocMotif
  val gMotifKeyToName = STMConf.atomocMotifKeyToName
  val gMotifNameToKey = STMConf.atomocMotifNameToKey
  val gMotifNameToOrbitKeys = STMConf.motifNameToOrbitKeys
  val gmotifNameToITeMKeys = STMConf.motifNameToITeMKeys
  var currItrID = 0
  var currWinID = 0
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


  def myprintln(str:String): Unit =
  {
  if(gDebug)
    println(str)
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
    val t_start = System.nanoTime()
    val a = Map(1->Map(101->5), 2->Map(101->6))
    val b = Map(1->Map(101->5), 2->Map(101->6), 3->Map(1->5))
    val c = a |+|b
    println(c)
    //System.exit(-1)

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
    myprintln("sep is " + sep)
    val nodeFile = clo.getOrElse("-input_file", "input-graph-file.csv")
    val avg_outdeg_file =
      clo.getOrElse("-avg_outdeg_file", nodeFile + "avg_outdeg.csv")

    val output_base_dir = clo.getOrElse("-base_out_dir", "./output/testout/")

    val max_cores = clo.getOrElse("-max_cores", "200")
    println("max core is ", max_cores)
    lazy val sparkConf = new SparkConf()
      .registerKryoClasses(Array.empty)
      .set("spark.default.parallelism",max_cores)
      .set("spark.sql.shuffle.partitions",max_cores)
      .set("spark.kryoserializer.buffer.max","2000m")
      .set("spark.sql.broadcastTimeout","10000000")
      .set("spark.network.timeout","10000000")
      .set("spark.driver.maxResultSize","30g").set("spark.local.dir","D:\\tmp\\")
    lazy val sparkSession = SparkSession
      .builder()
      .appName("STM").master("local")
      .config(sparkConf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sc = sparkSession.sparkContext
    val sqlc = sparkSession.sqlContext


    myprintln("input paramters are :" + clo.toString)
    myprintln("Spark paramters are " + sc.getConf.getAll.foreach(println))

    /*
     * Get the base tag rdd which has 4 things: src etype dst time
     *
     */
    //val inputTAG = TAGBuilder.init_rdd(nodeFile, sc, sep)



    val inputtag_varchartmp = TAGBuilder.init_tagrdd_varchar(nodeFile,sc,sep).cache()
    val inputtag_varchar = inputtag_varchartmp.filter(e=>(e._1 != "".hashCode) && (e._3 != "".hashCode )) .cache()
    inputtag_varchartmp.unpersist(true)
    val filterarr :Array[String] = clo.getOrElse("-filterset","").split(",")
    myprintln("filterset arr input "+ filterarr.toList)

    val node_id_label :Array[(vertexId, Array[Char])]= inputtag_varchar.flatMap(entry=>Iterator((entry._1,entry._7(0)),
      (entry._3,entry._7(1)))).distinct().collect()

    node_id_label.foreach(v=>nodemapFile.println(v._1 +","+ v._2.mkString))
    nodemapFile.flush()

    val filterNodeIDs_MaLo = inputtag_varchar.flatMap(entry=>{

      var filterNode = scala.collection.mutable.Set.empty[Int]
      val valset :List[String] = entry._7.map(k=>k.mkString("")).toList

      val filterset :Set[String] = filterarr.toSet
      for(filter <- filterset)
        {
          if(valset.indexOf(filter) %2 == 0) //src node
            filterNode += entry._1
          else if(valset.indexOf(filter) %2 == 1) //dst node
            filterNode += entry._3
        }
      filterNode
    }).distinct().collect()
    myprintln("filter node ids are "+ filterNodeIDs_MaLo.toList)
    myprintln("filter node ids len"+ filterNodeIDs_MaLo.length)

    val filterNodeIDs_WoLo = sc.broadcast(filterNodeIDs_MaLo).value
    val inputtag :TAGRDD = inputtag_varchar.map(e=> (e._1, e._2, e._3, e._4,0.0,Array.empty[Int],
                                                      Array.empty[Int]) ).cache()
    import gov.pnnl.datamodel.TAG
    val inputTAG = new TAG(inputtag)
    /*
     * Main method to get motif probability .It returns 3 results:
     *    * normMotifProb: normalized motif probability
     *    * offsetProb: time offset of the motifs
     *    * avg_out_deg: out degree distribution of the input graph
     */
    var local_res = processTAG(inputTAG, gDebug, clo,filterNodeIDs_WoLo)

    myprintln("local res 1" + local_res._1)

    //write json file
    // val out_file_os_path = new PrintWriter(new File(out_json_file_os_path))
    //    writeMotifPatterns.writeJSON(gAtomicMotifs.values.toArray, out_file_os_path, local_res._1,
    //                                 local_res
    //      ._2,
    //                                 duration, v_size)

    //write average out degree file and motif count json file
    gITeM_FreqFWr.println("]")
    gITeM_FreqFWr.flush()
    gOffsetAllFWriter.println("]")
    gOffsetAllFWriter.flush()
    writeAvgOutDegFile(avg_outdeg_file, local_res._3)

    try {
      moveFilesToOutdir(output_base_dir)
      }catch {
        case e: Exception => {
          myprintln("\nERROR: Moving file = ")
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          myprintln("\n Exception is  " + sw.toString())
        }
      }
    val t_end = System.nanoTime()
    println("Total time to run is " + (t_end - t_start) / 1000000000)
  }

  def processTAG(
      baseTAG: gov.pnnl.datamodel.TAG,
      gDebug: Boolean,
      clo: Map[String, String],
      filterNodeIDs: Array[vertexId]
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


    val deltaLimit: Boolean = clo.getOrElse("-delta_limit", "false").toBoolean

    val tDelta: Long = if(deltaLimit == true)
              clo.getOrElse("-t_delta", "600").toLong
    else
      Long.MaxValue

    val k_top = clo.getOrElse("-k_top", "4").toInt

    val gETypes =
      clo.getOrElse("-valid_etypes", "0").split(",").map(et => et.toInt)

    val max_cores = clo.getOrElse("-max_cores", "200").toInt

    val inputSimpleTAG = baseTAG.get_simple_tagrdd
    /*
     * Broacast the vertext arrival times to each cluster-node because it us used in look-up as
     * local Map
     */
    val vAppearanceTime: RDD[(Int, Long)] =
      this.get_vertex_birth_time(inputSimpleTAG,max_cores ).cache()
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
        inputSimpleTAG,
        tDelta,
        filterNodeIDs,
        k_top,
        max_cores
      )
      (res._1, res._2, avg_out_deg)
    } else {
      val res = complete_STM(gDebug, gETypes, inputSimpleTAG,filterNodeIDs,k_top,max_cores)
      (res._1, res._2, avg_out_deg)
    }

  }

  def findIsolatedVtx(g_base: GraphFrame,
                      motifName: String,
                      gETypes: Array[eType],
                      filterNodeIDs:Array[vertexId],max_cores:Int): GraphFrame = {

    if (gDebug) {
      println("graph sizev ", g_base.vertices.count)
      println("graph size e", g_base.edges.count)
    }

    val g = if(filterNodeIDs.length > 0 ) g_base.filterVertices( col("id").isin(filterNodeIDs: _*))
    else g_base
    var isolated_v = g.degrees
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

    /*
    update vertex orbit freque
     */
    //update vertex orbit frequency
    val vertex_orbit_freq :RDD[(Int,Array[Int])]= isolated_v.map(v=>{
      (v,Array(gMotifNameToOrbitKeys.get(motifName).get(1)))
      // to fix: we should not have two orbits for simultanious mutli-edges
    })
    //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. 0,1,2
    val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
    =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap

    gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map
    //update vertex ITem freq
    updateITemFreq("isolatednode",isolated_v.collect().toList,0)

    //write co occurennce
    isolated_v.collect().foreach(v=> gMotifVtxCooccurFWr.println(v + "," + v + "," + motifName))
    gMotifVtxCooccurFWr.flush()


    writeMotifVertexAssoication(isolated_v.collect(), motifName)

    write_vertex_independence(iso_v_cnt, iso_v_cnt)

    write_motif_independence(iso_v_cnt, iso_v_cnt)

    gMotifInfo += List(iso_v_cnt.toInt)
    myprintln(gMotifInfo.toString)
    //gOffsetInfo += List(0L)
    g_base.filterEdges("type != -1").dropIsolatedVertices()
  }

  def get_edge_from_row(row: Row): (Int, Int, Int, Long) = {
    (get_row_src(row), get_row_etype(row), get_row_dst(row), get_row_time(row))
  }

  def findIsolatedEdg(g_base: GraphFrame,
                      motifName: String,
                      gETypes: Array[eType],
                      filterNodeIDs:Array[vertexId],max_cores:Int): GraphFrame = {

    val spark = SparkSession.builder().getOrCreate()


    val g = if(filterNodeIDs.length > 0 ) g_base.filterEdges( col("src").isin(filterNodeIDs: _*) &&
      col("dst").isin(filterNodeIDs: _*))
        else g_base
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    import org.apache.spark.sql.functions._

    myprintln("in isolated edge")
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
      iso_edgs.select(selectEdgeArr.head, selectEdgeArr.tail: _*).persist()
    val iso_edge_cnt = selctedMotifEdges.count()
    // update vertex item freq
    val node_ids = selctedMotifEdges.rdd.flatMap(row
    =>Iterable(get_row_src(row),get_row_dst(row))).collect().toList
    updateITemFreq(motifName,node_ids , 0)

    //update vertex orbit frequeyncy.
    val vertex_orbit_freq :RDD[(Int,Array[Int])]= selctedMotifEdges.rdd.flatMap(e=>{
      Iterator((get_row_src(e),Array(gMotifNameToOrbitKeys.get(motifName).get(1))),
        (get_row_dst(e),Array(gMotifNameToOrbitKeys.get(motifName).get(2))))
      // to fix: we should not have two orbits for simultanious mutli-edges
    })
    //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. 0,1,2
    val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
    =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap

    gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map

    val tmi_edges_rdd: RDD[(Int, Int, Int, Long)] =
      selctedMotifEdges.rdd.map(row => get_edge_from_row(row))
    write_motif_vertex_association_file(tmi_edges_rdd, motifName)

    val newe = g_base.edges.except(selctedMotifEdges)
    val newv = g_base.vertices.except(v_deg_1)

    write_vertex_independence(iso_edge_cnt * 2, iso_edge_cnt * 2)
    write_motif_independence(iso_edge_cnt, iso_edge_cnt)

    gMotifInfo += List(iso_edge_cnt.toInt)
    //gOffsetInfo += List(0L)
    GraphFrame(newv, newe)
  }

  def findQuad(g: GraphFrame,
               motifName: String,
               gETypes: Array[eType], tDelta: Long,filterNodeIDs:Array[vertexId],max_cores:Int): GraphFrame = {
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
                    4,
                    tDelta,
                    filterNodeIDs,
                    max_cores
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
                    4,
                    tDelta,
                    filterNodeIDs,
                    max_cores
                  )

              //TODO: look at the need of this check and the return type
              if (validMotifsArray.isEmpty)
                break

              write_motif_vertex_association_file(validMotifsArray, motifName)

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
                  initial_tag: SimpleTAGRDD,
    dDelta: Long,filterNodeIDs:Array[vertexId],k_top:Int,max_cores:Int): GraphFrame = {
    val spark = SparkSession.builder().getOrCreate()


    val sqlc = spark.sqlContext


    val vInitialRDD = initial_tag
      .flatMap(nd => Iterator((nd._1, nd._1), (nd._3, nd._3)))
      .cache()
    import sqlc.implicits._
    val vDF = vInitialRDD.distinct.toDF("id", "name") .cache()
    import sqlc.implicits._

    vInitialRDD.unpersist(true)

    /*
     * we filter for edge type >= 0 because we use -1 edge type for isolated vertex
     * 1000 -1 1000 0 means 1000 is an isolated node
     */
    val multi_edges_TAG = findSimultaniousMultiEdges(initial_tag,filterNodeIDs,max_cores,"simulatanious" ).cache()

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

    // Create a GraphFrame
    import org.graphframes.GraphFrame
    var g = GraphFrame(vDF, eDF).cache()

    vDF.unpersist(true)

    val SYMMETRY = true
    val ASYMMETRY = false

    // Fix the isolated node calculation. exception in the file read create a -1 node which
    // is used by the code so there is 1 more isolated nodes than requried.
    g = findIsolatedVtx(g, "isolatednode", gETypes,filterNodeIDs,max_cores)
    g = findIsolatedEdg(g, "isolatededge", gETypes,filterNodeIDs,max_cores)
    g = findNonSimMultiEdg(g, "multiedge", gETypes,filterNodeIDs,max_cores)
    g = findSelfLoop(g, "selfloop", gETypes,filterNodeIDs,max_cores)
    g = findTriad(g, "triangle", SYMMETRY, gETypes,dDelta,filterNodeIDs,k_top,max_cores).cache()
    g = findTriad(g, "triad", ASYMMETRY, gETypes,dDelta,filterNodeIDs,k_top,max_cores).cache()
    g = findQuad(g, "twoloop", gETypes,dDelta,filterNodeIDs,max_cores).cache()
    g = findQuad(g, "quad", gETypes,dDelta,filterNodeIDs,max_cores)
    g = findDyad(g, "loop", SYMMETRY, gETypes, 2, 2,dDelta,filterNodeIDs,max_cores).cache()
    g = findTriad(g, "outstar", SYMMETRY, gETypes,dDelta,filterNodeIDs,k_top,max_cores).cache()
    g = findTriad(g, "instar", SYMMETRY, gETypes,dDelta,filterNodeIDs,k_top,max_cores).cache()
    g = findDyad(g, "outdiad", SYMMETRY, gETypes, 3, 2,dDelta,filterNodeIDs,max_cores).cache()
    g = findDyad(g, "indiad", SYMMETRY, gETypes, 3, 2,dDelta,filterNodeIDs,max_cores).cache()
    g = findDyad(g, "inoutdiad", ASYMMETRY, gETypes, 3, 2,dDelta,filterNodeIDs,max_cores).cache()
    g = findResidualEdg(g, "residualedge", gETypes,filterNodeIDs,max_cores).cache()

    if (gDebug) {
      println("FINAL after residual graph sizev ", g.vertices.count)
      println("graph size e", g.edges.count)
    }
    g

  }

  def jsonStringFlattten(gMotifInfo: ListBuffer[eType]): String = {
    "" +  "\"m0\":[" + gMotifInfo.slice(0,3).mkString(",") + "]," +
             "\"m1\":[" + gMotifInfo.slice(3,4).mkString(",") + "],"+
             "\"m2\":[" + gMotifInfo.slice(4,5).mkString(",") + "]," +
             "\"m3\":[" + gMotifInfo.slice(5,6).mkString(",") + "],"+
             "\"m4\":[" + gMotifInfo.slice(6,8).mkString(",") + "],"+
             "\"m5\":[" + gMotifInfo.slice(8,12).mkString(",") + "],"+
             "\"m6\":[" + gMotifInfo.slice(12,16).mkString(",") + "],"+
             "\"m7\":[" + gMotifInfo.slice(16,20).mkString(",") + "],"+
             "\"m8\":[" + gMotifInfo.slice(20,25).mkString(",") + "],"+
             "\"m9\":[" + gMotifInfo.slice(25,28).mkString(",") + "],"+
             "\"m10\":[" + gMotifInfo.slice(28,33).mkString(",") + "],"+
             "\"m11\":[" + gMotifInfo.slice(33,38).mkString(",") + "],"+
             "\"m12\":[" + gMotifInfo.slice(38,42).mkString(",") + "],"+
             "\"m13\":[" + gMotifInfo.slice(42,46).mkString(",") + "],"+
             "\"m14\":[" + gMotifInfo.slice(46,50).mkString(",") + "],"+
             "\"m15\":[" + gMotifInfo.slice(50,52).mkString(",") + "]"


  }
  def jsonString(gMotifInfo: ListBuffer[List[eType]],key :String= "m"): String = {

    var jsonstr :StringBuilder = new StringBuilder("")
    for(i <- 0 until gMotifInfo.length)
    {
      if(i == gMotifInfo.length - 1 )
        jsonstr.append("\""+ key + i+ "\":[" + gMotifInfo(i).mkString(",") + "]")
      else
        jsonstr.append("\""+key + i+ "\":[" + gMotifInfo(i).mkString(",") + "],")
    }
    jsonstr.toString()
  }

  def jsonStringLong(gOffsetInfo: ListBuffer[List[Long]],key:String="m"): String = {

    var jsonstr: StringBuilder = new StringBuilder("")
    for (i <- 0 until gOffsetInfo.length) {
      if (i == gOffsetInfo.length - 1)
        jsonstr.append("\""+key+ i + "\":[" + gOffsetInfo(i).mkString(",") + "]")
      else
        jsonstr.append("\""+key + i + "\":[" + gOffsetInfo(i).mkString(",") + "],")
    }
    jsonstr.toString()
  }
  def jsonStringDouble(gMotifOrbitIndInfo: ListBuffer[List[Double]],key:String="m"): String = {

    var jsonstr: StringBuilder = new StringBuilder("")
    for (i <- 0 until gMotifOrbitIndInfo.length) {
      if (i == gMotifOrbitIndInfo.length - 1)
        jsonstr.append("\""+key+ i + "\":[" + gMotifOrbitIndInfo(i).mkString(",") + "]")
      else
        jsonstr.append("\""+key + i + "\":[" + gMotifOrbitIndInfo(i).mkString(",") + "],")
    }
    jsonstr.toString()
  }

  def write_vertex_ITeM_Orbti_Frequency(gVertex_ITeM_Freq: Map[vertexId, Map[vertexId, vertexId]],
                                        gVertex_Orbit_Freq: Map[vertexId, Map[vertexId, vertexId]],
                                        itr: Int, w: Int) :Unit =
    {
      for((vid,item_freq) <- gVertex_ITeM_Freq)
      {
        gVertexITeMFreqFWr.print(itr+","+w+","+vid)
        /*
         *
         * get vertex - ITeM frequency
         * [v_id->[item_id->frequency]]
         */
        for(i<-0 to 51)
        {
          //verte item freq
          val vif = item_freq.getOrElse(i,0)
          gVertexITeMFreqFWr.print(","+vif)
        }
        gVertexITeMFreqFWr.println()
      }
      gVertexITeMFreqFWr.flush()

      // output vertex orbit frequency
      for((vid,item_freq) <- gVertex_Orbit_Freq)
      {
        gVertexOrbitFreqFWr.print(itr+","+w+","+vid)
        /*
         *
         * get vertex - ITeM frequency
         * [v_id->[item_id->frequency]]
         */
        for(i<-0 to 28)
        {
          //verte item freq
          val vif = item_freq.getOrElse(i,0)
          gVertexOrbitFreqFWr.print(","+vif)
        }
        gVertexOrbitFreqFWr.println()
      }
      gVertexOrbitFreqFWr.flush()

    }

  def complete_STM(
      gDebug: Boolean,
      gETypes: Array[Int],
      initial_simple_tag: SimpleTAGRDD,filterNodeIDs:Array[vertexId],k_top:Int,max_cores:Int
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

      // Header line of few files
    gITeM_IndFWr.println(1+","+1)
      gVtxIndFWr.println(1+","+1)
      gMotifVtxAssoFWr.println(1+","+1)
      gOrbtVtxAssoFWr.println(1+","+1)
    gVtxIndFWr.println(
      "#num_v_nonverlapping,num_v_max,v_independence_0_0")
    gITeM_IndFWr.println(
      "#num_total_motif,num_ind_motif,motif_independence_0_0"
    )

    try {
      val g = findAllITeM(gETypes, call_id, initial_simple_tag,  duration,filterNodeIDs,k_top,max_cores)
      if (gDebug) {
        println(gMotifInfo.toList)
        println("number of edges in last graph", g.edges.count)
        println("number of vertex in last graph", g.vertices.count)
      }
    } catch {
      case e: Exception => {
        myprintln("\nERROR: Call id = " + call_id)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())
      }
    }

    /*
     * Write current GMotifInfo to the "All" file
     */
    gITeM_FreqFWr.println("{ \"itr\":" + 0 + ",\"w\":" + 0 + "," + jsonString(gMotifInfo) +
                             "}")
    gITeM_FreqFWr.flush()
    gOffsetAllFWriter.println("{ \"itr\":" +  0 + ",\"w\":" + 0 + "," + jsonStringLong(gOffsetInfo) + "}")
    //gOffsetAllFWriter.println(
      //1 + "," + 1 + "," + gOffsetInfo.flatten.mkString(",")
    //)
    gOrbit_Ind_FWr.println("{ \"itr\":" +  0 + ",\"w\":" + 0 + "," + jsonStringDouble(gOrbit_Ind,"orb") + "}")
    gOrbit_Ind_FWr.flush()
    gOffsetAllFWriter.flush()

    /*
     * Generate Output
     * 1. Motif Probability
     * 2. Edge Offset Probability
     * 3. Vertex ITeM Frequency File
     */
    val normMotifProb: ListBuffer[Double] =
      gMotifInfo.flatMap(f0 => f0.map(f1 => f1.toDouble / duration))
    gITeMRateFWr.println(normMotifProb.mkString("\n"))


    val offsetProb: ListBuffer[Long] =
      gOffsetInfo.flatMap(f0 => f0.map(f1 => f1))
    gOffsetFWriter.println(offsetProb.mkString("\n"))

      write_vertex_ITeM_Orbti_Frequency(gVertex_ITeM_Freq, gVertex_Orbit_Freq, 0,0)

    /*
     * Output files
     */
    gITeMRateFWr.flush()
    gOffsetFWriter.flush()

    (normMotifProb, offsetProb)
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
      initial_simple_tag: SimpleTAGRDD,
      tDelta:Long,
      filterNodeIDs: Array[vertexId],
      k_top:Int,
      max_cores:Int



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

    if (!gDebug) {
      println("min time", minTime)
      println("max time", maxTime)
      println("duration in milliseconds", duration)
    }

    val num_windows: Int = sampling_population
    val time_in_window: Long = duration / num_windows
    val total_edges: Long = initial_simple_tag.filter(e => e._4 > -1).count()
    var window_prob: ListBuffer[Double] = ListBuffer.empty

    //val deg_File = new PrintWriter(new File("Deg_Dist.csv"))
    for (i <- 0 to num_windows - 1) {
      val win_start_time = minTime + i * time_in_window
      val win_end_time = minTime + (i + 1) * time_in_window
      myprintln("win start and end "+ win_start_time+ win_end_time)


      val all_edges_in_current_window = initial_simple_tag
        .filter(
          e =>
            (e._4 >= win_start_time) //start and end time does not include -1
              && (e._4 < win_end_time)
        )
//      val deg_dist = all_edges_in_current_window.flatMap(e
//      =>Iterator((e._1,1),(e._3,1))).reduceByKey((d1,d2)=>d1+d2).map(v=>v._2).collect()
//
//      deg_File.println(deg_dist.mkString(","))
//      deg_File.flush()
//
      val edges_in_current_window: Long =
        all_edges_in_current_window.count()
      myprintln(" edges in current window i = "+ i + " " + edges_in_current_window)
      window_prob += edges_in_current_window.toDouble / total_edges
    }
    myprintln("prob is " + window_prob)
    gWindowSizeFWriter.println(window_prob.mkString(","))
    gWindowSizeFWriter.flush()
    var gMotifInfo_global = ListBuffer.empty[Double]
    var gOffsetInfo_global = ListBuffer.empty[Long]
    // Header line of few files
    gITeM_IndFWr.println(num_iterations+","+num_windows)
    gVtxIndFWr.println(num_iterations+","+num_windows)
    gMotifVtxAssoFWr.println(num_iterations+","+num_windows)
    gOrbtVtxAssoFWr.println(num_iterations+","+num_windows)
    for (itr <- 0 to num_iterations - 1) {
      currItrID = itr
      var gMotifInfo_itr_local = ListBuffer.empty[Double]
      var gOffsetInfo_itr_local = ListBuffer.empty[Long]
      var num_w_in_sampling = 0

      for (i <- 0 to num_windows -1) {
        currWinID = i
        val t0 = System.nanoTime()
        val rn = scala.util.Random
        if ((rn.nextDouble() < sample_selection_prob) || currWinID == 0) //forcing i==0 so that atleast one is picked
          {
            myprintln(" i is " + i)
            gVtxIndFWr.println(
              "#num_v_nonverlapping,num_v_max,v_independence_" + itr + "_" + i
            )
            gITeM_IndFWr.println(
              "#num_total_motif,num_ind_motif," +
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
                (e._4 >= (minTime + i * time_in_window)) &&
                  (e._4 < (minTime + (i + 1) *
                    time_in_window))
            )
            var call_id = 0
            try {
              findAllITeM(gETypes, call_id, local_tag,tDelta,filterNodeIDs,k_top,max_cores)
            } catch {
              case e: Exception => {
                myprintln("\nERROR: Call id = " + call_id)
                val sw = new StringWriter
                e.printStackTrace(new PrintWriter(sw))
                myprintln("\n Exception is  " + sw.toString())
              }
            }

            /*
             * Write current GMotifInfo to the "All" file
             */
            val CLOSING_BRACKET = if(i == num_windows -1) "}" else "},"
            gITeM_FreqFWr.println("{ \"itr\":" +  itr + ",\"w\":" + i + "," + jsonString(gMotifInfo) + CLOSING_BRACKET)
            //gMotifAllProbFWr.println(
              //itr + "," + i + "," + gMotifInfo.flatten.mkString(",")
            //)
            gITeM_FreqFWr.flush()
            gOffsetAllFWriter.println("{ \"itr\":" +  itr + ",\"w\":" + i + "," + jsonStringLong(gOffsetInfo,"off") + CLOSING_BRACKET)
            //gOffsetAllFWriter.println(
              //itr + "," + i + "," + gOffsetInfo.flatten.mkString(",")
            //)
            gOffsetAllFWriter.flush()

            //gMotifOrbitAllFWr.println(itr + "," + i + "," + gMotifOrbitInfo.flatten.mkString(","))
            gOrbit_Ind_FWr.println("{ \"itr\":" +  itr + ",\"w\":" + i + "," + jsonStringDouble(gOrbit_Ind,"orb") + CLOSING_BRACKET)
            gOrbit_Ind_FWr.flush()

            write_vertex_ITeM_Orbti_Frequency(gVertex_ITeM_Freq, gVertex_Orbit_Freq, itr, i)
            gVertex_ITeM_Freq = scala.collection.Map.empty
            gVertex_Orbit_Freq = scala.collection.Map.empty

            // gMotifInfo gOffsetInfo has counts for local graph
            if (gMotifInfo_itr_local.isEmpty) {
              //motif info is "rate of that motif devided by probability"
              // SO we need to first compute "rate" which is count/time_in_window
              gMotifInfo_itr_local = gMotifInfo.flatten
                .map(f1 => f1.toDouble / time_in_window)
                .map(m => (m / window_prob(i)))

              myprintln(
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

              /*
               * Should not normalize offset time with the probability of the snapshot.
               * Offset is the exact value of duration time even in the sampled snapshot
               */
              val weighted_offset_info =
                //  gOffsetInfo.flatten.map(m => (m / window_prob(i)).toLong)
                gOffsetInfo.flatten.map(m => m)
              gOffsetInfo_itr_local =
                gOffsetInfo_itr_local.zip(weighted_offset_info).map {
                  case (x, y) => x + y
                }
              myprintln(
                "gmotif info is empty " + gMotifInfo_itr_local.mkString("&&")
              )
            }
            gMotifInfo.clear
            gOffsetInfo.clear
            //both are reset because for next local graph motif computation, they should start
            // with empty values
          }
        val t1 = System.nanoTime()
        val windnowTime = (t1 - t0)
        // write time taken in this window to the file
        gWindowTimeFWriter.println(itr + "," + i + "," + windnowTime)
        gWindowTimeFWriter.flush()
      }
      gMotifInfo_itr_local =
        gMotifInfo_itr_local.map(m => m / num_w_in_sampling)

      //Averaging offset time is fine because in every sample, we have added corresponding times
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
     * Average out global result for all the iterations
     */
    gMotifInfo_global = gMotifInfo_global.map(m => m / num_iterations)
    gOffsetInfo_global = gOffsetInfo_global.map(o => o / num_iterations)
    gITeMRateFWr.println(gMotifInfo_global.mkString("\n"))
    gOffsetFWriter.println(gOffsetInfo_global.mkString("\n"))
    /*
     * Output files
     */
    gITeMRateFWr.flush()
    gOffsetFWriter.flush()
    (gMotifInfo_global, gOffsetInfo_global)
  }

  def findSimultaniousMultiEdges(inputSimpleTAG: SimpleTAGRDD,filterNodeIDs:Array[vertexId],max_cores:Int,motif_name :String): SimpleTAGRDD = {

    val sim_e_base = if(filterNodeIDs.length > 0){ inputSimpleTAG.filter(e
      => filterNodeIDs.contains(e._1) && filterNodeIDs.contains(e._3))}
    else
     { inputSimpleTAG }
    val sim_e = sim_e_base.map(e => (e, 1))
        .reduceByKey((c1, c2) => c1 + c2,max_cores)
        .filter(e => e._2 > 1)
        .cache()

    val sim_e_vpairs = sim_e.flatMap(e => Iterator(e._1._1, e._1._3)).distinct(max_cores)
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

        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }

        //write_vertex_independence(sim_e_num_v, sim_e_max_num_v)
        write_vertex_independence(0,0)
        write_motif_independence(0,0)

        // TODO : Make sure it is written even for empty simultanious edges
        // write_motif_vertex_association_file(sim_e.keys, "simulatanious")

        return inputSimpleTAG
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())

        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        write_vertex_independence(sim_e_num_v, sim_e_max_num_v)
        write_motif_independence(0,0)

        write_motif_vertex_association_file(sim_e.keys, "simulatanious")

        return inputSimpleTAG

    }

    if (gHigherGOut == true) {
      // Only show the source node where we have simultanious edges
      sim_e.collect
        .foreach(e => gHigherGraphFWriter.println(e._1))
      gHigherGraphFWriter.flush()
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
        //update vertex item frequency
        updateITemFreq(motif_name,node_ids,numReusedNodes)
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

    //update vertex orbit frequency
    val vertex_orbit_freq :RDD[(Int,Array[Int])]= sim_e_vpairs.flatMap(v=>{
      Iterator((v,Array(0)),(v,Array(1)))
      // to fix: we should not have two orbits for simultanious mutli-edges
    })
    //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. 0,1,2
    val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
    =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap

    gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map


    val local_motif_info = reuse_node_info.values.toList
    gMotifInfo += local_motif_info
    myprintln(gMotifInfo.toString())

    write_vertex_independence(sim_e_num_v, sim_e_max_num_v)

    // This is always 1.
    write_motif_independence(1,1)
    write_motif_vertex_association_file(sim_e.keys, "simulatanious")
    inputSimpleTAG.distinct(max_cores)
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
  ,max_cores:Int): RDD[(Int, Long)] = {

    /*
     * compute first appearance time of each vertex.
     * (vid,etime)===> get smallest etime for a give vid
     */
    nodeQuadruples
      .flatMap(nd => Iterator((nd._1.toInt, nd._4), (nd._3.toInt, nd._4)))
      .reduceByKey((time1, time2) => Math.min(time1, time2),max_cores)
  }

  def findNonSimMultiEdg(g_base: GraphFrame,
                         motifName: String,
                         gETypes: Array[Int],
                         filterNodeIDs:Array[vertexId],max_cores:Int): GraphFrame = {
    val spark = SparkSession.builder().getOrCreate()

    val g = if(filterNodeIDs.length > 0 ) g_base.filterEdges( col("src").isin(filterNodeIDs: _*) &&
      col("dst").isin(filterNodeIDs: _*))
    else g_base

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
            gMotifInfo += List(0)
            gOffsetInfo += List(-1L,-1L,-1L)
            write_vertex_independence(0,0)
            write_motif_independence(0,0)

            return tmpG
          }
        } catch {
          case e: Exception =>
            val sw = new StringWriter
            e.printStackTrace(new PrintWriter(sw))
            myprintln("\n Exception is  " + sw.toString())
            gMotifInfo += List(0)
            gOffsetInfo += List(-1L,-1L,-1L)
            write_vertex_independence(0,0)
            write_motif_independence(0,0)

            return tmpG

        }
        /*
         * TODO : better way should be implemented than converting DF to RDD and then RDD to DF back
         * just to do flattening of edges.
         */
        val validMotifs =
          selctedMotifEdges.rdd.setName("validMotifMultiEdge").cache()
        /* get "representative edge" of the multi edges. for a given src,dst edge this is the
         * edge with lowest timestamp We create a set of edges for a given source-dst pair a
         * nd at the same time compute the smallest edge.  The first element i.e. the set gives
         * us the count of multi-edges and the 2nd element gives us the edges to keep.  Total
         * multi-edges are the sum of all 1st elements. We do -1 because for every edge set, there
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
            },max_cores)
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

        val eSum = avg_offset_time_perkey.sum
        val eCnt = avg_offset_time_perkey.count
        val eMean: Long = eSum.toLong / eCnt
        val gSC: SparkContext = SparkSession.builder.getOrCreate().sparkContext
        val eMeanLocal = gSC.broadcast(eMean).value
        val devs = avg_offset_time_perkey.map(offset => (offset - eMeanLocal) * (offset - eMeanLocal))
        val stddev = if (eCnt == 1) Math.sqrt(devs.sum / eCnt)
                     else Math.sqrt(devs.sum / (eCnt - 1))

        gOffsetInfo += List(eMean,stddev.toLong,eCnt)

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
          multi_edges_info.flatMap(mi => mi._2._1 - mi._2._2).cache()

        print("multi_edges_to_remove size is " + multi_edges_to_remove.count())
        //update vertex orbit frequency
        //update vertex orbit frequeyncy.i some of the code is common. make method
        val a = multi_edges_to_remove.flatMap(e=>Iterator((e._1,1),(e._3,1)))
        println("a is " + a.count())
        val vertex_orbit_freq :RDD[(Int,Array[Int])] = multi_edges_to_remove.flatMap(e=>{
          Iterator((e._1,Array(5)),
            (e._3,Array(6)))
        }).cache()
//        multi_edges_to_remove.flatMap(e
//        =>Iterator((e._1,Array(gMotifNameToOrbitKeys.get(motifName).get(1))),
//            (e._3,Array(gMotifNameToOrbitKeys.get(motifName).get(2))))
//          // to fix: we should not have two orbits for simultanious mutli-edges
//        )
        //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. -1,1,2
        val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
        =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap

        gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map



        /*
         * write multi-edge nodes to a file
         */
        val multi_edge_nodes = multi_edges_to_remove
          .flatMap(me => {
            Iterable(me._1, me._3)
          })
          .distinct(max_cores)
          .collect()

        // get node ids for item frequency update
        val node_ids = multi_edge_nodes.toList
        updateITemFreq(motifName,node_ids,2)
        //vertex Ind depends on how many unique vertices exists
        val multi_edge_nodes_cnt = multi_edge_nodes.length
        val max_possible_multi_edge_nodes = 2 * multi_edges_to_remove.count()
        write_vertex_independence(multi_edge_nodes_cnt,max_possible_multi_edge_nodes)

        //motif independence is always 1 here if it exists : TODO: Remove this line after some review
        write_motif_independence(8,8)

        myprintln("total multi edges are : " +total_multi_edges)
        myprintln("avg avg_offset_time is " + eMean)
        val multi_edges_df = sqlc.createDataFrame(multi_edges_to_remove)

        // For reuse/-max_cores_node_info: For every motif, both the nodes are reused for the 2nd edge
        // So the resuling map it (2-> number of multi edges)
        val reuse_node_info: Map[Int, Int] = Map(2 -> total_multi_edges)
        gMotifInfo += reuse_node_info.values.toList

        //TODO: Motif association for Non-Simu Multi-edges

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

        val newEDF = g_base.edges.except(multi_edges_df)
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

  def findSelfLoop(g_base: GraphFrame,
                   motifName: String,
                   gETypes: Array[Int],
                   filterNodeIDs:Array[vertexId],max_cores:Int): GraphFrame = {

    val spark = SparkSession.builder().getOrCreate()
    val g = if(filterNodeIDs.length > 0 ) g_base.filterVertices( col("id").isin(filterNodeIDs: _*))
    else g_base

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

      val gSC: SparkContext = SparkSession.builder.getOrCreate().sparkContext
      val gVBirthTime_exec = gSC.broadcast(gVBirthTime).value

      val new_self_loop_v =selctedMotifEdges
        .filter(row => {
          val v = get_row_src(row)
          val t = get_row_time(row)
          gVBirthTime_exec.getOrElse(v, -2) == t
        }).persist()
      val new_self_loop_cnt = new_self_loop_v.count().toInt
      val reuse_self_loop_v =selctedMotifEdges
        .filter(row => {
          val v = get_row_src(row)
          val t = get_row_time(row)
          gVBirthTime_exec.getOrElse(v, -2) != t
        }).persist()
      //update item freq
      val new_node_ids = new_self_loop_v.rdd.map(row=>get_row_src(row)).collect().toList
      val reuse_node_ids = reuse_self_loop_v.rdd.map(row=>get_row_src(row)).collect().toList

      updateITemFreq(motifName,new_node_ids,0)
      updateITemFreq(motifName,reuse_node_ids,1)

      //update vertex orbit frequency
      val vertex_orbit_freq :RDD[(Int,Array[Int])]= selctedMotifEdges.rdd.map(e=>{
        (get_row_src(e),Array(gMotifNameToOrbitKeys.get(motifName).get(1)))
      })
      //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. 0,1,2
      val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
      =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap

      gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map



      // edge offset is not relevent here
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

      self_loop_nodes.foreach(v=> gMotifVtxCooccurFWr.println(v + "," + v + "," + motifName))
      gMotifVtxCooccurFWr.flush()
      writeMotifVertexAssoication(self_loop_nodes, motifName)

      val v_distinct = self_loop_nodes.length
      if(v_distinct == 0)
        {
          write_vertex_independence(0, 0)
          write_motif_independence(0,0)
        }else
      {
        write_vertex_independence(v_distinct, total_self_loop_cnt)
        write_motif_independence(6,6)
      }

      val newEDF = g_base.edges.except(selctedMotifEdges)
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
      gMotifInfo += List(new_self_loop_cnt, reuse_self_loop_cnt)
      // unpersist old graph
      tmpG.unpersist(true)
      g.unpersist(true)
      tmpG = newGraph
    }
    tmpG
  }

  /*
   * write a motif's nodes to a file
   */
  def write_motif_vertex_association_file(
      validMotifsArray: RDD[(Int, Int, Int, Long)],
      motifName: String
  ): Unit = {
    /*use this method as entry point to write motif
    co-occurence file also
     */
    validMotifsArray.collect().foreach(e=>
    gMotifVtxCooccurFWr.println(e._1 + "," + e._3 + "," + motifName))
    gMotifVtxCooccurFWr.flush()


    val multi_edge_nodes: Array[Int] = validMotifsArray
      .flatMap(e => {
        Iterator(e._1, e._3)
      })
      .distinct()
      .collect()

    writeMotifVertexAssoication(multi_edge_nodes, motifName)

  }

  def writeMotifVertexAssoication(allV: Array[Int], motifName: String): Unit = {
    gMotifVtxAssoFWr.println(
      currItrID + "," +
        currWinID + "," +
        gMotifNameToKey(motifName) + "," +
        allV.mkString(",")
    )
    gMotifVtxAssoFWr.flush()
  }

  def findTriad(g: GraphFrame,
                motifName: String,
                symmetry: Boolean = false,
                gETypes: Array[Int],tDelta: Long, filterNodeIDs:Array[vertexId],k_top:Int,max_cores:Int): GraphFrame = {

//    println("check if graph g e is chached " + g.edges.storageLevel.useMemory)

    var tmpG = g
    for (et1 <- gETypes.indices) {
      for (et2 <- gETypes.indices) {
        for (et3 <- gETypes.indices) {
          breakable {
            if (gDebug) {
              println("graph triad sizev ", g.vertices.count)
              println("graph size e", g.edges.count)
            }
            myprintln("finding moding " + motifName)
              val res =
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
                  3,
                  tDelta,
                  filterNodeIDs,
                  k_top,
                  max_cores
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
                  3,
                  tDelta,
                  filterNodeIDs,
                  k_top,
                  max_cores
                )

            val validMotifsArray: RDD[(Int, Int, Int, Long)] = res._2
            tmpG = res._1
            if (validMotifsArray.isEmpty)
              break

            write_motif_vertex_association_file(validMotifsArray, motifName)
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

  def updateITemFreq(motif_name: String, node_ids: List[vertexId], numReusedNodes: Int) :Unit=
  {
    /*
         *
         * get vertex - ITeM frequency
         * [v_id->[item_id->frequency]]
         */
    var vertex_item_freq : Map[Int,Map[Int,Int]] = scala.collection.Map.empty
    /*

         setup vertex -item -freq
          */
    //println("motif name is " + motif_name)
    val possible_ITEM_IDs = gmotifNameToITeMKeys.get(motif_name)
    val ITeM_ID = possible_ITEM_IDs.get(numReusedNodes)
    for(nid <- node_ids)
    {
      val existing_freq_map :Map[Int,Int] = vertex_item_freq.getOrElse(nid,scala.collection.Map.empty)
      // Every node belong th this ITeM_ID
      val updated_freq = existing_freq_map.getOrElse(ITeM_ID,0) + 1
      val update_freq_map = existing_freq_map + (ITeM_ID -> updated_freq)
      vertex_item_freq = vertex_item_freq + (nid ->update_freq_map)
      // now frequency of a specific ITeM_Id is updated
    }
    gVertex_ITeM_Freq = gVertex_ITeM_Freq |+| vertex_item_freq
    //println("after item ferq" + gVertex_ITeM_Freq.toString())
  }


  def get_node_reuse_info_from_mis_motif(
      num_motif_nodes: vertexId,
      num_motif_edges: vertexId,
      mis_set: RDD[String],
      motif_name :String
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
        /*
        this causes bug for triad so use the else part only that
        applies to all situations
        val node_ids =
          if (num_motif_nodes == num_motif_edges)
            all_edges_arrs.map(e => e(0).toInt).toList
          else
          */

          val node_ids =
            all_edges_arrs
              .flatMap(e => Set(e(0).toInt, e(2).toInt))
              .toSet
              .toList
/*
        if(Array("outdiad","indiad","inoutdiad","outstar", "instar","triad") contains motif_name)
          {
           // println(" Printing " + motif_name)
            val dyadGraphFile = new PrintWriter(new FileWriter(t1 + "_"+motif_name + "_dyad.txt",true ))
            all_edges_arrs.foreach(e=>dyadGraphFile.println(e.toList))
            dyadGraphFile.flush()


            val dyadNodeFile =new PrintWriter(new FileWriter(t1 + "_NodeList_"+motif_name + "_dyad.txt",true ))
            node_ids.foreach(n=>dyadNodeFile.println(n))
            dyadNodeFile.flush()
          }
*/
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
             9.223946460926997motifNameToOrbitKeysE-6
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

        updateITemFreq(motif_name,node_ids,numReusedNodes)


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

  def write_orbit_association(orbit_vertex_asso: Array[(vertexId, Set[vertexId])], motifName: String):Unit =
    {
      /*
      * Write orbit_vertex_association file
      */
      val orbitIDMapMotif = gMotifNameToOrbitKeys(motifName)
      orbit_vertex_asso.foreach(ova => {
        gOrbtVtxAssoFWr.println(
          currItrID + "," + currWinID + "," +
            +orbitIDMapMotif(ova._1) + "," + ova._2.mkString(",")
        )
      })
      gOrbtVtxAssoFWr.flush()
    }

  def updateOrbitFreq(motifName: String, orbit_vertex: RDD[(vertexId, Array[vertexId])],max_cores:Int) :Unit =
    {
      //println("motif name is " + motifName)
      val vertex_orbit_freq = orbit_vertex.map(ov=>{
        val size1_arr = ov._2
        (size1_arr(0),Array(gMotifNameToOrbitKeys.get(motifName).get(ov._1)))
      })
      //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. 0,1,2
      val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
      =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap
      //print("Before global " + gVertex_Orbit_Freq)
      //print("Before local" + vertex_orbit_freq_map)
      gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map
      //print("After global " + gVertex_Orbit_Freq)
    }

  def writeOrbitIndependence_VertexAssociation(true_mis_set_rdd: RDD[String],
                                               num_nonoverlapping_m: Long,
                                               motifName: String, max_cores:Int): Unit = {
    /*
       "simultanious edge" -> : ALWAYS ONE SO DON'T WRITE
       "non-sim multi edge" -> " ALWAYS ONE SON DONT WRITE
       "residualedge" -> "(a)-[e1]->(b)",: ALWAYS ONE SO DONT WRITE
       "selfloop" -> "(a)-[e1]->(b)",ALWAYS ONE SO DONT WRITE
       "multiedge" -> "(a)-[e1]->(b); (a)-[e2]->(b)",
       "isolatednode" -> "a", ALWAYS ONE SO DONT WRITE
       "isolatededge" -> "(a)-[e1]->(b)", ALWAYS (1,1) SO DONT WRITE


       In output:
       orb0:triable, 1:triad 2:twoloop 3:quad 4:loop 5:outstar 6:instar 7:outdiad
       8:indiad 9:inoutdiad
       "orb0":[NaN],"orb1":[NaN,NaN,NaN],"orb2":[NaN,NaN],"orb3":[NaN],
       "orb4":[NaN,NaN], "orb5":[1.0,2.0],"orb6":[1.0,2.0]}

       "orb0":[NaN],"orb1":[-8.3,NaN,NaN],"orb2":[-6.6,NaN],"orb3":[-5.5],
       "orb4":[-2.5],"orb5":[-1.6,NaN],"orb6":[1.0,2.0],"orb7":[-3.5,NaN],
       "orb8":[1.0,2.0],"orb9":[-4.5,NaN,NaN]}}
     */
    if (motifName.equalsIgnoreCase("triangle") ||
        motifName.equalsIgnoreCase("quad") ||
        motifName.equalsIgnoreCase("loop")) {
      // only one orbit
      val numVOrbit = get_edges_from_mis_motif(true_mis_set_rdd)
        .map(edge => edge._1)
        .distinct()
        .count
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_vertex: RDD[(Int, Array[Int])] = motif_edges
        .flatMap(edge_arr => {
          var ov_frm_this_edge = new ArrayBuffer[(Int,Array[Int])]()
          //howeer many edges we have, take the first node. since it is a symmetic motif
          for(e <- edge_arr)
            ov_frm_this_edge += ((1,Array(e.split("_")(0).toInt)))
          ov_frm_this_edge
        })
        .distinct()

      updateOrbitFreq(motifName,orbit_vertex,max_cores)

      gOrbit_Ind += List(numVOrbit.toDouble / num_nonoverlapping_m)
    } else if (motifName.equalsIgnoreCase("triad")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_vertex: RDD[(Int, Array[Int])] = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          val e3 = edge_arr(2).split("_")
          Iterator((1, Array(e1(0).toInt)),
                   (2, Array(e2(0).toInt)),
                   (3, Array(e3(0).toInt)))
        })
        .distinct()

      val orbit_vertex_asso   = orbit_vertex.reduceByKey((x, y)
      => x ++ y,max_cores).map(ov=>(ov._1, ov._2.toSet)).collect

      //We need to convert it to list and then get a map of frequency to write vertex_orbit_freqency
      updateOrbitFreq(motifName,orbit_vertex,max_cores)

      write_orbit_association(orbit_vertex_asso,motifName)

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
      gOrbit_Ind += orbit_independence
    } else if (motifName.equalsIgnoreCase("outdiad") ||
               motifName.equalsIgnoreCase("indiad") ||
               motifName.equalsIgnoreCase("twoloop")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_vertex : RDD[(Int, Array[Int])] =
        if(motifName.equalsIgnoreCase("outdiad"))
        {
          motif_edges
            .flatMap(edge_arr => {
              val e1 = edge_arr(0).split("_")
              val e2 = edge_arr(1).split("_")
              //there are two orbits, a and b|c
              Iterator((1, Array(e1(0).toInt)),
                (2, Array(e1(2).toInt , e2(2).toInt)))
            })
            .distinct()
        } else if(motifName.equalsIgnoreCase("indiad"))
          {
            motif_edges
              .flatMap(edge_arr => {
                val e1 = edge_arr(0).split("_")
                val e2 = edge_arr(1).split("_")
                //there are two orbits, a and b|c
                Iterator((1, Array(e1(2).toInt)),
                  (2, Array(e1(0).toInt , e2(0).toInt)))
              })
              .distinct()
          }else // twoloop
          {
            motif_edges
              .flatMap(edge_arr => {
                val e1 = edge_arr(0).split("_")
                val e2 = edge_arr(1).split("_")
                //there are two orbits, b and a|c
                Iterator((1, Array(e1(2).toInt)),
                  (1, Array(e1(0).toInt , e2(2).toInt)))
              })
              .distinct()
          }

      updateOrbitFreq(motifName,orbit_vertex,max_cores)


      val orbit_vertex_asso = orbit_vertex.reduceByKey((x, y) => x ++ y,max_cores).map(ov
      =>(ov._1,ov._2.toSet)).collect()
      write_orbit_association(orbit_vertex_asso,motifName)

      val orbit_count: Map[Int, Int]  = orbit_vertex_asso
        .map(x => (x._1, x._2.size))
        .toMap
      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m
      )
      gOrbit_Ind += orbit_independence
    } else if (motifName.equalsIgnoreCase("inoutdiad")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))
      val orbit_vertex : RDD[(Int, Array[Int])] = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          //there are three orbits, a,b,c
          Iterator((1, Array(e1(0).toInt)),
            (2, Array(e1(2).toInt)),
            (3, Array(e2(2).toInt)))
        })
        .distinct()

      updateOrbitFreq(motifName,orbit_vertex,max_cores)

      val orbit_vertex_asso = orbit_vertex.reduceByKey((x, y) => x ++ y,max_cores).map(ov
      =>(ov._1,ov._2.toSet)).collect()
      write_orbit_association(orbit_vertex_asso,motifName)
      val orbit_count: Map[Int, Int] = orbit_vertex_asso
        .map(x => (x._1, x._2.size))
        .toMap

      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(3, 0).toDouble / num_nonoverlapping_m
      )
      gOrbit_Ind += orbit_independence
    } else if (motifName.equalsIgnoreCase("instar") ||
               motifName.equalsIgnoreCase("outstar")) {
      val motif_edges = true_mis_set_rdd.map(motif => motif.split('|'))

      val orbit_vertex : RDD[(Int, Array[Int])] = motif_edges
        .flatMap(edge_arr => {
          val e1 = edge_arr(0).split("_")
          val e2 = edge_arr(1).split("_")
          val e3 = edge_arr(2).split("_")
          //there are two orbits, a and b|c|d
          Iterator(
            (1, Array(e1(0).toInt)),
            (2, Array(e1(2).toInt)),
            (2, Array(e2(2).toInt)),
            (2, Array(e3(2).toInt))
          )
        })
        .distinct()

      updateOrbitFreq(motifName,orbit_vertex,max_cores)
      val orbit_vertex_asso = orbit_vertex.reduceByKey((x, y) => x ++ y,max_cores).map(ov
      =>(ov._1,ov._2.toSet)).collect()
      write_orbit_association(orbit_vertex_asso,motifName)

      val orbit_count: Map[Int, Int] = orbit_vertex_asso
        .map(x => (x._1, x._2.size))
        .toMap

      val orbit_independence = List(
        orbit_count.getOrElse(1, 0).toDouble / num_nonoverlapping_m,
        orbit_count.getOrElse(2, 0).toDouble / num_nonoverlapping_m
      )
      gOrbit_Ind += orbit_independence
    }
  }


  def getEdgeOffsetMeanSDev(
      reuse_temporal_offset_info: ArrayBuffer[ArrayBuffer[eTime]]
  ): ArrayBuffer[timeOffset] = {
    reuse_temporal_offset_info.map(edgeInfo => {
      val eSum = edgeInfo.sum
      val eCnt = edgeInfo.length
      val eMean: Long = eSum / eCnt
      val devs = edgeInfo.map(offset => (offset - eMean) * (offset - eMean))
      val stddev = if (eCnt == 1) Math.sqrt(devs.sum / eCnt)
                   else Math.sqrt(devs.sum / (eCnt - 1))
      new timeOffset(eMean, stddev, eCnt)
    })
  }

  def base4EFind(graph: GraphFrame,motifName:String,gETypes:Array[Int],
    et1: eType,
    et2: eType,
    et3: eType,
    et4: eType) : Dataset[Row] ={
   graph.find(gAtomicMotifs(motifName))
     .filter(
              (col("a") =!= col("b")) &&
              (col("b") =!= col("c")) &&
              (col("a") =!= col("c"))
            )
     .filter("e1.type = " + gETypes(et1))
     .filter("e2.type = " + gETypes(et2))
     .filter("e3.type = " + gETypes(et3))
     .filter("e4.type = " + gETypes(et4))
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
      num_motif_edges: Int,
      tDelta : Long,
    filterNodeIDs:Array[vertexId],max_cores:Int
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
        filterNodeID_4E(base4EFind(tmpG,motifName,gETypes,et1,et2,et3,et4), filterNodeIDs,
                        num_motif_nodes)
        .cache()

      else
              filterNodeID_4E(base4EFind(tmpG,motifName,gETypes,et1,et2,et3,et4).filter(
              (col("c") =!= col("d")) &&
              (col("d") =!= col("a")) &&
              (col("b") =!= col("d"))
          )
          .filter("e1.time < e2.time"),filterNodeIDs,num_motif_nodes)
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
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(3*(num_motif_edges - 1)) { -1 }
        write_vertex_independence(0,0)
        write_motif_independence(0,0)
        if (
            motifName.equalsIgnoreCase("quad")){
          gOrbit_Ind += List(Double.NaN)
        }else if (motifName.equalsIgnoreCase("twoloop")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN)
        }
        return sc.emptyRDD
      }
    } catch {
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(3*(num_motif_edges - 1)) { -1 }
        write_vertex_independence(0,0)
        write_motif_independence(0,0)
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
      motifName,max_cores
    )
    val validMotifsArray: RDD[(Int, Int, Int, Long)] =
      get_edges_from_mis_motif(true_mis_set_rdd).cache()

    val reuse_node_info: Map[Int, Int] = get_node_reuse_info_from_mis_motif(
      num_motif_nodes,
      num_motif_edges,
      true_mis_set_rdd,
      motifName
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

    myprintln("quad " + gMotifInfo)
    myprintln("quad time" + gOffsetInfo)
    // Get time offset infor
    val cnt_validMotifs = true_mis_set_rdd.count()
    updateEdgeOffset(true_mis_set_rdd, num_motif_edges)

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
    true_mis_set_rdd
  }

  def get_edge_time_offset_info_from_mis_motifs(
      num_motif_edges: Int,
      mis_set: RDD[String]
  ): ArrayBuffer[ArrayBuffer[Long]] = {
    val reuse_temporal_offset_info: ArrayBuffer[ArrayBuffer[Long]] =
      mis_set
        .map(motifid => {

          val all_edges_ids = motifid.split('|')
          val all_edges_arrs: ArrayBuffer[Array[String]] = ArrayBuffer.empty

          // all_edges_arrs is an array(size of motif) of array(always size 4)
          for (i <- 0 until num_motif_edges) {
            all_edges_arrs.append(all_edges_ids(i).split("_"))
          }

          val local_reuse_temporal_offset_info: ArrayBuffer[ArrayBuffer[Long]] =
            ArrayBuffer.fill(num_motif_edges - 1)(ArrayBuffer[Long]())

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
            local_reuse_temporal_offset_info(i) = ArrayBuffer(
              all_edges_arrs(i + 1)(3).toLong - all_edges_arrs(i)(3).toLong)

          }
          local_reuse_temporal_offset_info
        })
        .treeReduce((arr1, arr2) => {
          //val a = arr
          /*
           * Some arr1 has 3 offests (quad), some 2 (triag) some 1 (diad)
           *
           * in order to compute mean and standard deviation , we need to create
           * maintain an array of all the values
           */
          var res = ArrayBuffer[ArrayBuffer[Long]]()
          for (i <- 0 until arr1.length)
            res += (arr1(i) ++ arr2(i))
          //append two arraybuffers
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

  def topKStar(
      tmpG: GraphFrame,
      num_motif_nodes: Int,
      num_motif_edges: Int,
      gSQLContext: SQLContext,
      motifName: String,
      k_top:Int
  ): (Map[Int, Int], ArrayBuffer[Long], GraphFrame) = {
    val outheader = Array("id", "outDegree")
    val inheader = Array("id", "inDegree")
    val k = k_top
    import org.apache.spark.sql.functions._
    val f = new PrintWriter(new File("sneakyGraph_beforetopK_edges.csv"))
    tmpG.edges.collect.foreach(e=>f.println(get_row_src(e) + ","+get_row_etype(e)
      + ","+get_row_dst(e) + ","  + get_row_time(e)))
    f.flush()
    val topKOut: Array[Row] =
      if (motifName.equalsIgnoreCase("outstar"))
        tmpG.outDegrees
          .select(outheader.head, outheader.tail: _*)
          .orderBy(desc("outDegree"))
          .limit(20000)
          .rdd
          .collect()
      else
        tmpG.inDegrees
          .select(inheader.head, inheader.tail: _*)
          .orderBy(desc("inDegree"))
          .limit(20000)
          .rdd
          .collect()

    val topK_V = topKOut.map(row => (row.getAs[Int](0)))
    myprintln("top k are "+ k+ " " + topK_V.toList)
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
          //myprintln(partId+ local_star_map.toString())
          local_high_star_motifs.toIterator
        })

        .cache()

    /*val topkfile = new PrintWriter(new FileWriter("topk_"+motifName + ".txt",true))
    global_high_star_motifs.collect().foreach(eset=>{
      for(e <- eset)
        topkfile.println(get_row_id(e))
    })
    topkfile.flush()

     */
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
        Map((0 -> 0), (1 -> 0), (2 -> 0), (3 -> 0), (4->0))
      // one less motif value in 2807670993968171MotifProb_AbsCount_Individualkdd.txt
      //7.0.18_delta/higgsPartial becuase the default_motif_info is only up to 3->0
      val defaul_timeoffset: ArrayBuffer[Long] = ArrayBuffer(-1L, -1L)
      return (default_motif_info, defaul_timeoffset, tmpG)

    }
    val cnt_validMotifs_star = true_mis_set_rdd_star.count

    val avg_reuse_temporal_offset_info =
      getGlobalTimeOffsetList(true_mis_set_rdd_star, num_motif_edges)

    val validMotifsArray_star: RDD[(Int, Int, Int, Long)] =
      get_edges_from_mis_motif(true_mis_set_rdd_star).cache()

    val reuse_node_info_star = get_node_reuse_info_from_mis_motif(
      num_motif_nodes,
      num_motif_edges,
      true_mis_set_rdd_star,
      motifName
    )

    val filteredTmpG =
      get_new_graph_except_processed_motifs_edges(tmpG, validMotifsArray_star)
        .cache()
    (reuse_node_info_star, avg_reuse_temporal_offset_info, filteredTmpG)
  }

  def base3EFind(newGraph: GraphFrame, motifName :String, gETypes:Array[Int],
    et1: eType,
    et2: eType,
    et3: eType):Dataset[Row] = {
    newGraph.find(gAtomicMotifs(motifName))
      .filter("a != b")
      .filter("b != c")
      .filter("e1.type = " + gETypes(et1))
      .filter("e2.type = " + gETypes(et2))
      .filter("e3.type = " + gETypes(et3))
  }



  def filterNodeID_3E(valueSoFar: Dataset[Row],
                      filterNodeIDs: Array[vertexId],
                      num_motif_nodes: Int): Dataset[Row] = {
    if (filterNodeIDs.length == 0)
      valueSoFar
    else
      valueSoFar.filter(
          col("a.id").isin(filterNodeIDs: _*)
            && col("b.id").isin(filterNodeIDs: _*)
            && col("c.id").isin(filterNodeIDs: _*)
        )

  }
  def filterNodeID_4E(valueSoFar: Dataset[Row],
    filterNodeIDs: Array[vertexId],
    num_motif_nodes: Int): Dataset[Row] = {
    if (filterNodeIDs.length == 0)
      valueSoFar
    else
      if (num_motif_nodes == 4)
               valueSoFar.filter(
                                  col("a.id").isin(filterNodeIDs: _*)
                                  && col("b.id").isin(filterNodeIDs: _*)
                                  && col("c.id").isin(filterNodeIDs: _*)
                                  && col("d.id").isin(filterNodeIDs: _*)
                                )
             else
               valueSoFar.filter(
                                  col("a.id").isin(filterNodeIDs: _*)
                                  && col("b.id").isin(filterNodeIDs: _*)
                                  && col("c.id").isin(filterNodeIDs: _*)
                                )

  }
  def filterNodeID_2E(valueSoFar: Dataset[Row],
                      filterNodeIDs: Array[vertexId],
                      num_motif_nodes: Int): Dataset[Row] = {
    if (filterNodeIDs.length == 0)
      valueSoFar
    else
      if (num_motif_nodes == 3)
        valueSoFar.filter(
          col("a.id").isin(filterNodeIDs: _*)
            && col("b.id").isin(filterNodeIDs: _*)
            && col("c.id").isin(filterNodeIDs: _*)
        )
      else
        valueSoFar.filter(
          col("a.id").isin(filterNodeIDs: _*)
            && col("b.id").isin(filterNodeIDs: _*)
        )

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
                         num_motif_edges: Int,
    tDelta : Long,filterNodeIDs: Array[vertexId],k_top:Int,max_cores:Int):
  (GraphFrame, RDD[(Int, Int, Int, Long)]) = {

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    var resultantGraph = tmpG

    var reuse_node_info_star = Map[Int, Int]()
    for (i <- 0 to num_motif_nodes)
      reuse_node_info_star += (i -> 0)
    var avg_reuse_temporal_offset_info_star = ArrayBuffer[Long]()

    /*
    val filtere1e2 = "(e1.time - e2.time) < " + tDelta
    val filtere1e2_min = "(e1.time - e2.time) > -" + tDelta
    val filtere2e3 = "(e2.time - e3.time) < " + tDelta
    val filtere2e3_min = "(e2.time - e3.time) > -" + tDelta
    val filtere3e1 = "(e3.time - e1.time) < " + tDelta
    val filtere3e1_min = "(e3.time - e1.time) > -" + tDelta
    .filter(filtere1e2)
          .filter(filtere1e2_min)
          does not work. SO adding every condition manually for GT benchmarking
          TODO:
          //          .filter("(e1.time - e2.time) < 600")
//          .filter("(e1.time - e2.time) > -600" )
//          .filter("(e2.time - e3.time) < 600")
//          .filter("(e2.time - e3.time) > -600" )
//          .filter("(e3.time - e1.time) < 600")
//          .filter("(e3.time - e1.time) > -600" )
          //.filter((col("e1.time") - col("e2.time")).between(-tDelta, tDelta)  )
          //.filter((col("e2.time") - col("e3.time")).between(-tDelta, tDelta)  )
          //.filter((col("e3.time") - col("e1.time")).between(-tDelta, tDelta)  )
          // reducing candidate two loop i.e. azc or cza=> pick only azc
          // time based restriction wont work for this motif type
          //.filter("e1.time < e2.time")
          //.filter("e2.time < e3.time")
     */

    val overlappingMotifs =
      if (num_motif_nodes == 4) {

        myprintln("original graph e " + tmpG.edges.count())
        val res =
          topKStar(tmpG, num_motif_nodes, num_motif_edges, sqlc, motifName,k_top)
        reuse_node_info_star = res._1
        avg_reuse_temporal_offset_info_star = res._2
        val newGraph = res._3
        newGraph.cache()

        myprintln("Sneaky graph e "+ newGraph.edges.count())
        val f = new PrintWriter(new File("sneakyGraph_edges.csv"))
        newGraph.edges.collect.foreach(e=>f.println(get_row_src(e) + ","+get_row_etype(e)
          + ","+get_row_dst(e) + "," + get_row_time(e)))
        f.flush()

        resultantGraph = newGraph
        filterNodeID_3E(
          base3EFind(newGraph, motifName, gETypes, et1, et2, et3)
            .filter("c != d")
            .filter("a != d")
            .filter("a.id < c.id"),
          filterNodeIDs,num_motif_nodes
        ).cache()

      } else if (symmetry)
        filterNodeID_3E(
          base3EFind(tmpG, motifName, gETypes, et1, et2, et3)
            .filter("c != a")
            .filter("e1.time < e2.time"),
          filterNodeIDs,num_motif_nodes
        ).cache()
      //.filter("e2.time < e3.time").cache()
      else
        filterNodeID_3E(
          base3EFind(tmpG, motifName, gETypes, et1, et2, et3)
            .filter("c != a"),
          filterNodeIDs,num_motif_nodes
        ).cache()
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
    myprintln("starting caching")
    overlappingMotifs.repartition(max_cores)
    overlappingMotifs.cache()
    myprintln("done caching. starting count")

    val num_overlap_motifs = overlappingMotifs.count()
    val selctedMotifEdges_local_nonoverlap =
      get_local_NO_motifs(overlappingMotifs, selectEdgeArr, sqlc).cache()

    // get unique motif
    try {
      if (selctedMotifEdges_local_nonoverlap.head(1).isEmpty) {
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(3*(num_motif_edges - 1)) { -1L }
        write_vertex_independence(0,0)
        write_motif_independence(0,0)
        if (motifName.equalsIgnoreCase("triangle")
        ) {
          gOrbit_Ind += List(Double.NaN)
        }else if (motifName.equalsIgnoreCase("triad")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN,Double.NaN)
        }else if (motifName.equalsIgnoreCase("instar") ||
          motifName.equalsIgnoreCase("outstar")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN)
        }
        return (resultantGraph,sc.emptyRDD)
      }
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(3*(num_motif_edges - 1)) { -1L }
        write_vertex_independence(0,0)
        write_motif_independence(0,0)
        if (motifName.equalsIgnoreCase("triangle")
            ) {
          gOrbit_Ind += List(Double.NaN)
        }else if (motifName.equalsIgnoreCase("triad")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN,Double.NaN)
        }else if (motifName.equalsIgnoreCase("instar") ||
                  motifName.equalsIgnoreCase("outstar")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN)
        }
        return (resultantGraph, sc.emptyRDD)
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
      motifName,max_cores
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
      true_mis_set_rdd,
      motifName
    )

    myprintln("sneaky star gMotif " + reuse_node_info_star.values.toList)
    myprintln("Non sneaky star gMotif " + reuse_node_info.values.toList)

    val local_res = (reuse_node_info_star.values.toList
      .zip(reuse_node_info.values.toList))
      .map {
        case (x, y) => x + y
      }
   gMotifInfo += local_res

    write_motif_independence(num_overlap_motifs, num_nonoverlap_motifs)
    write_vertex_independence(
      v_distinct_cnt,
      num_nonoverlap_motifs * num_motif_nodes
    )
    // Get time offset information
    val cnt_validMotifs = true_mis_set_rdd.count()

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      getGlobalTimeOffsetList(true_mis_set_rdd, num_motif_edges)
    gOffsetInfo += (avg_reuse_temporal_offset_info.toList
      .zip(avg_reuse_temporal_offset_info.toList))
      .map { case (x, y) => x + y }

    myprintln("edge offset at triad"+ gOffsetInfo)


    (resultantGraph, validMotifsArray)
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
      myprintln(
        " selctedMotifEdges_NonOverRDD count is " + selctedMotifEdges_NonOverRDD
          .count()
      )
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())

      }
    }
    val motif_schme = selctedMotifEdges.schema

    selctedMotifEdges.unpersist(true)
    import gSQLContext.implicits._
    gSQLContext.createDataFrame(selctedMotifEdges_NonOverRDD, motif_schme)

  }

  def getGlobalTimeOffsetList(true_mis_set_rdd: RDD[String],
                              num_motif_edges: eType): ArrayBuffer[Long] = {
    val reuse_temporal_offset_info: ArrayBuffer[ArrayBuffer[Long]] =
      get_edge_time_offset_info_from_mis_motifs(
        num_motif_edges,
        true_mis_set_rdd
      )

    //Create timeOffset objects. reuse_temporal_offset_info should be either of
    // size 2, 3, or 4. edgeInfo is a ArrayBuffer[Long] for a each edge in the
    // temporal motif
    val timeOffsetInfo: ArrayBuffer[timeOffset] = getEdgeOffsetMeanSDev(
      reuse_temporal_offset_info)

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      timeOffsetInfo.flatMap(
        to =>
          ArrayBuffer(to.mean.toLong,
                      to.stdDev.toLong,
                      to.populationSize.toLong)
      )
    avg_reuse_temporal_offset_info
  }

  /*
   * Update global gOffsetInfo
   */
  def updateEdgeOffset(true_mis_set_rdd: RDD[String],
                       num_motif_edges: eType) = {

    val avg_reuse_temporal_offset_info: ArrayBuffer[Long] =
      getGlobalTimeOffsetList(true_mis_set_rdd, num_motif_edges)

    gOffsetInfo += avg_reuse_temporal_offset_info.toList

  }

  def base2EFile(tmpG: GraphFrame, gETypes: Array[vertexId], et1: eType, et2: eType,
                 motifName:String): Dataset[Row] = {
   tmpG
      .find(gAtomicMotifs(motifName))
      .filter("a != b")
      .filter("e1.type = " + gETypes(et1))
      .filter("e2.type = " + gETypes(et2))
  }

  def find2EdgNVtxMotifs(tmpG: GraphFrame,
                         motifName: String,
                         symmetry: Boolean = false,
                         et1: eType,
                         et2: eType,
                         gETypes: Array[Int],
                         num_motif_nodes: Int,
                         num_motif_edges: Int,
                         tDelta : Long, filterNodeIDs:Array[vertexId],max_cores:Int): RDD[(Int, Int, Int, Long)] = {
    myprintln(" Staring 2e3v motif nV, vE"+ num_motif_nodes+ num_motif_edges)
    val spark = SparkSession.builder().getOrCreate()


    val sc = spark.sparkContext
    val sqlc = spark.sqlContext

    val overlappingMotifs =
      if (num_motif_nodes == 2) {
        if (symmetry)
          filterNodeID_2E(base2EFile(tmpG,gETypes,et1,et2,
            motifName).filter("e1.time < e2.time"),filterNodeIDs,num_motif_nodes)
            .cache()
        else
         filterNodeID_2E(base2EFile(tmpG,gETypes,et1,et2,motifName),filterNodeIDs,num_motif_nodes)     .cache()

      } else {
        if (symmetry)
          filterNodeID_2E(base2EFile(tmpG,gETypes,et1,et2,motifName)
            .filter("c != a")
            .filter("e1.time < e2.time"),filterNodeIDs,num_motif_nodes)
            .cache()
        else
          filterNodeID_2E(base2EFile(tmpG,gETypes,et1,et2,motifName)
            .filter("c != a"),filterNodeIDs,num_motif_nodes)
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
        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(3*(num_motif_edges - 1)) { -1 }
        write_vertex_independence(0,0)
        write_motif_independence(0,0)
        if (
          motifName.equalsIgnoreCase("loop")) {
          gOrbit_Ind += List(Double.NaN)
        }else if (motifName.equalsIgnoreCase("outdiad") ||
          motifName.equalsIgnoreCase("indiad")
        ){
          gOrbit_Ind += List(Double.NaN,Double.NaN)
        }else if (motifName.equalsIgnoreCase("inoutdiad")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN,Double.NaN)
        }
        return sc.emptyRDD
      }
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())

        gMotifInfo += List.fill(num_motif_nodes + 1) { 0 }
        gOffsetInfo += List.fill(3*(num_motif_edges - 1)) { -1 }
        write_vertex_independence(0,0)
        write_motif_independence(0,0)
        if (
            motifName.equalsIgnoreCase("loop")) {
          gOrbit_Ind += List(Double.NaN)
        }else if (motifName.equalsIgnoreCase("outdiad") ||
                  motifName.equalsIgnoreCase("indiad")
                      ){
          gOrbit_Ind += List(Double.NaN,Double.NaN)
        }else if (motifName.equalsIgnoreCase("inoutdiad")) {
          gOrbit_Ind += List(Double.NaN,Double.NaN,Double.NaN)
        }

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
      MaximumIndependentSet.getMISGreedy(valid_motif_overlap_graph).cache()
    /* mis-set is:
     * 1612_588_1355184824|588_589_1357159776|1612_589_1355764972,1612_588_1355184824|588_589_1357159776|1612_589_1357153244
     */
    val true_mis_set_rdd = get_local_NO_after_MIS(mis_set, sc).cache()
    val num_nonoverlapping_m = true_mis_set_rdd.count()
    writeOrbitIndependence_VertexAssociation(
      true_mis_set_rdd,
      num_nonoverlapping_m,
      motifName,max_cores
    )

    val validMotifsArray: RDD[(Int, Int, Int, Long)] = get_edges_from_mis_motif(
      true_mis_set_rdd
    )

    valid_motif_overlap_graph.unpersist(true)

    /*
     * construct motif from edges to compute their information content
     *
     */
    val reuse_node_info: Map[Int, Int] =
      get_node_reuse_info_from_mis_motif(
        num_motif_nodes,
        num_motif_edges,
        true_mis_set_rdd,
        motifName
      )

    gMotifInfo += reuse_node_info.values.toList

    myprintln("Writing motif ind in 2ENv")
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
    updateEdgeOffset(true_mis_set_rdd, num_motif_edges)

    true_mis_set_rdd.unpersist(true)
    validMotifsArray
  }

  def findDyad(g: GraphFrame,
               motifName: String,
               symmetry: Boolean = false,
               gETypes: Array[Int],
               num_motif_nodes: Int,
               num_motif_edges: Int,
    tDelta: Long,filterNodeIDs:Array[vertexId],max_cores:Int): GraphFrame = {
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
          num_motif_edges,
          tDelta,filterNodeIDs,max_cores

        ).cache()
        write_motif_vertex_association_file(validMotifsArray, motifName)

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

  def get_row_id(row:Row) : String = {
    get_row_src(row)+ "_" + get_row_etype(row) + "_" + get_row_dst(row) + "_" + get_row_time(row)
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
  def findResidualEdg(g_base: GraphFrame,
                      motifName: String,
                      gETypes: Array[Int],
                      filterNodeIDs: Array[vertexId],max_cores:Int): GraphFrame = {

    val g = if(filterNodeIDs.length > 0 ) g_base.filterVertices( col("id").isin(filterNodeIDs: _*))
    else g_base

    if (gDebug) {
      println("graph sizev ", g.vertices.distinct.count)
      println("graph size e", g.edges.count)
    }

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
          gMotifInfo += List(0,0)

          // vertext INDE is always 1 as these are residual edgea and all the v will be distict
          // If not distinct then would have been part of an earlier motif
          // Same for motif_independen
          write_vertex_independence(0,0)
          write_motif_independence(0,0)
          //gOffsetInfo += List(0L)
          return tmpG
        }
      } catch {
        case e: Exception => {
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          myprintln("\n Exception is  " + sw.toString())
          gMotifInfo += List(0,0)
          write_vertex_independence(0,0)
          write_motif_independence(0,0)
          //gOffsetInfo += List(0L)
          return tmpG
        }
      }
/*
      val resi_edge_file = new PrintWriter(new FileWriter(t1+"residuel_edges.txt", true))
      selctedMotifEdges.collect().foreach(row=>resi_edge_file.println(get_row_id(row)))
      resi_edge_file.flush()
  */
      /*
       * write residual nodes to a file
       */
      val resi_edge_nodes = selctedMotifEdges.rdd
        .flatMap(row => {
          Iterable(get_row_src(row), get_row_dst(row))
        })
        .distinct()
        .collect()

      //write co-occurennce
      resi_edge_nodes.foreach(v=> gMotifVtxCooccurFWr.println(v + "," + v + "," + motifName))
      gMotifVtxCooccurFWr.flush()

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
       * BUG: Even both nodes can be "new" . TODO :fix it
       */
      if (gHigherGOut == true) {
        // Write all residual edges
        selctedMotifEdges.collect
          .foreach(row => gHigherGraphFWriter.println(get_row_id(row)))
        gHigherGraphFWriter.flush()
      }
      //BUG : evn residual edge can have both "new" nodes.
      val at_least_one_new_nodes_edges = selctedMotifEdges
        .filter(row => {
          val src = get_row_src(row)
          val dst = get_row_dst(row)
          val etime = get_row_time(row)
          if ((gVBirthTime.getOrElse(src, -1) == etime) || (gVBirthTime
            .getOrElse(dst, -1) == etime))
            true
          else false
        }).persist()

      val one_new_nodes_motif_cnt = at_least_one_new_nodes_edges.count().toInt

      val both_reused_nodes_edges = selctedMotifEdges.filter(row => {
          val src = get_row_src(row)
          val dst = get_row_dst(row)
          val etime = get_row_time(row)
          if ((gVBirthTime.getOrElse(src, -1) == etime) || (gVBirthTime
            .getOrElse(dst, -1) == etime))
            false
          else true
        }).persist()

      //print("residueal edge . one node reuse count " + one_new_nodes_motif_cnt )
      //print("residueal edge . both nodes reuse count " + both_reused_nodes_edges.count().toInt )

      //update vertex orbit frequency
      val tmprdd = selctedMotifEdges.rdd.cache()
      val vertex_orbit_freq :RDD[(Int,Array[Int])]= tmprdd.flatMap(e=>{
        Iterator((get_row_src(e),Array(27)),
          (get_row_dst(e),Array(28)))
      })
      //get orbit map from motif name, then get orbit key based on the oribin position in this motif i.e. 0,1,2
      val vertex_orbit_freq_map : Map[Int,Map[Int,Int]] = vertex_orbit_freq.reduceByKey((x, y) => x ++ y,max_cores).map(vo
      =>(vo._1, vo._2.toList.groupBy(identity).mapValues(_.size))).collect.toMap

      gVertex_Orbit_Freq = gVertex_Orbit_Freq |+| vertex_orbit_freq_map


      // update vertex item frequency
      val new_node_ids = at_least_one_new_nodes_edges.rdd.flatMap(row=>Iterable(get_row_src(row),
        get_row_dst(row))).distinct().collect().toList
      val reuse_node_ids = both_reused_nodes_edges.rdd.flatMap(row=>Iterable(get_row_src(row),
        get_row_dst(row))).distinct().collect().toList
      updateITemFreq(motifName,new_node_ids,0)
      updateITemFreq(motifName,reuse_node_ids,1)

      write_motif_independence(num_residual_edges, num_residual_edges)
      write_vertex_independence(num_residual_edges * 2, num_residual_edges * 2)
      // total number of nodes in residual edges are 2*number of edges because IF NOT they are
      // not residual edge but a wedge
      val reused_node_cnt =
        (num_residual_edges - one_new_nodes_motif_cnt).toInt

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
      moveFileInner(gITeMRateFile)
      moveFileInner(gITeM_FreqFile)
      moveFileInner(gOffsetFile)
      moveFileInner(gOffsetAllFile)
      moveFileInner(gVertexBirthFile)
      moveFileInner(gITeM_IndFile)
      moveFileInner(gVtxIndFile)
      moveFileInner(gHigherGraphFile)
      moveFileInner(nodemapFileObj)

      val directory = new File(".")
      myprintln("curr dir is "+ directory.getAbsolutePath)
      val pattern = "^.*" + t1 + ".*.txt$"
      myprintln("\nFiles that match regular expression: " + pattern)
      val filter: FileFilter = new RegexFileFilter("^.*" + t1 + ".*.txt$")
      val files = directory.listFiles(filter)

      myprintln("Files to move are "+ files.toList)
      files.foreach(afile => moveFileInner(afile))
    } catch {
      case e: Exception => {
        println("\nERROR: Failed to move files = ")
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        myprintln("\n Exception is  " + sw.toString())
      }
    }

  }

  def write_motif_independence(overlapping_cnt: Long,
                               non_overlapping_cnt: Long): Unit = {
    // write motif uniqueness file
    gITeM_IndFWr.println(
        non_overlapping_cnt + "," +
        overlapping_cnt + "," +
        non_overlapping_cnt.toDouble / overlapping_cnt.toDouble
    )
    gITeM_IndFWr.flush()
  }

  def write_vertex_independence(num_v_nonoverlapping: Long,
                                num_v_max_possible: Long) = {
    gVtxIndFWr.println(
      num_v_nonoverlapping + "," + num_v_max_possible + "," +
        num_v_nonoverlapping.toDouble / num_v_max_possible.toDouble
    )
    gVtxIndFWr.flush()
  }
}
