package gov.pnnl.stm.algorithms

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.SparkContext
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.graphframes.GraphFrame

import scala.util.Random
import util.control.Breaks._


object MaximumIndependentSet {

  def getOverlapGraph(dataframe:DataFrame,gSQLContext:SQLContext,threshold:Int): GraphFrame ={
  /*
   * input dataframe is a list of overlapping motif instances found
   * required output is "overlap graph" which has nodes as the overlapping
   * instances and the edges is any shared edge between them if exists
   * dataframe format is as follows
   * val selectEdgeArr = Array("e1.src", "e1.type", "e1.dst", "e1.time",
      "e2.src", "e2.type", "e2.dst", "e2.time",
      "e3.src", "e3.type", "e3.dst", "e3.time")
   *
   * create a rdd where key is edgeid and values are set of all the motifs
   * associated with it. then we will form a motif node and connect then with
   * this edge
   *
   */

    // dataframe.map is giving null pointer for vertices
    import gSQLContext.implicits._
    val overlap_graph_vertices = dataframe.rdd.map(row
      => (getMotifId(row))).toDF("id").cache()

    println("verlap graph vertices count " , overlap_graph_vertices.count())

    val edge_overlap :RDD[(String,Set[String])] = dataframe.rdd.flatMap(row =>{
    val motif_id:String = getMotifId(row)
      if(row.length == 16)
        {
          Iterable((getEdgeId(row,0),Set(motif_id)),
                   (getEdgeId(row,1),Set(motif_id)),
                   (getEdgeId(row,2),Set(motif_id)),
                   (getEdgeId(row,3),Set(motif_id)))

        }
      else if(row.length == 12)
        {
          Iterable((getEdgeId(row,0),Set(motif_id)),
            (getEdgeId(row,1),Set(motif_id)),
            (getEdgeId(row,2),Set(motif_id)))

        }else
        {
          Iterable((getEdgeId(row,0),Set(motif_id)),
            (getEdgeId(row,1),Set(motif_id)))
        }
  }).reduceByKey((s1,s2) => (s1 ++ s2))
    val overlap_graph_edges : RDD[(String,String)] = edge_overlap.flatMap(edge_motifs=>{
      var all_motifs :Array[String]= edge_motifs._2.toArray

      val len = all_motifs.length
      val local_edge : scala.collection.mutable.Set[(String,String)] = scala.collection.mutable.Set.empty
      if(len > 1)
        {
          for(i <- 0 to len-2)
          {
            for(j <- i+1 to len-1)
            {
              local_edge.add((all_motifs(i),all_motifs(j)))
              local_edge.add((all_motifs(j),all_motifs(i)))
            }
          }
        }
      local_edge
    }
    ).setName("overlap_grpah_edges").cache()
    import gSQLContext.implicits._
    val eDF = overlap_graph_edges.toDF("src","dst")
    val g = GraphFrame(overlap_graph_vertices,eDF)
    edge_overlap.unpersist(true)
    overlap_graph_edges.unpersist(true)
    return g
  }

  def getMotifEdges(row:Row): Set[String] =
  {
    if(row.length == 16) return Set(getEdgeId(row,0),
               getEdgeId(row,1),
               getEdgeId(row,2),
               getEdgeId(row,3))
    else if(row.length == 12)
           return Set(getEdgeId(row,0),
                    getEdgeId(row,1),
                    getEdgeId(row,2))

         else
            return Set(getEdgeId(row,0),
                     getEdgeId(row,1))
  }
  def getEdgeId(row: Row, eindex: Int): String =
  {
    return row.getAs[Int](eindex * 4).toString + "_" +
           row.getAs[Int](eindex * 4 + 1).toString  + "_" +
           row.getAs[Int](eindex * 4 + 2).toString  + "_" +
           row.getAs[Int](eindex * 4 + 3).toString
  }

  def getMotifId(row: Row): String =
  {
    /*
     * motif id is abct1t2t3
     */
    if(row.length == 16)
      {
        return getEdgeId(row,0).toString + "|" +
               getEdgeId(row,1).toString + "|" +
               getEdgeId(row,2).toString + "|" +
               getEdgeId(row,3).toString
      }
    else if(row.length == 12)
      {
        return getEdgeId(row,0).toString + "|" +
               getEdgeId(row,1).toString + "|" +
               getEdgeId(row,2).toString
      }else if(row.length == 8)
      {
        return getEdgeId(row,0).toString + "|" +
               getEdgeId(row,1).toString

      }else
      {
        return getEdgeId(row,0).toString
      }
  }

  def getMISGreedy(dataFrame: GraphFrame): RDD[String]  = {
    var g_dash : GraphFrame = dataFrame.cache()
    /*
    1. I = ∅, G′ = G
    2. While G′ is not the empty graph
        (a) Choose a random set of vertices S ⊆ V by selecting each vertex v independently with probability P r(v). Suppose P r(v) = 1 , where dv ≡ degree of v.
             dv
        (b) For every edge (u, v) ∈ E(G′) if both endpoints are in S, then remove the vertex of lower
            degree from S (break ties). Denote the set after this step as S′.
        (c) Remove S′ and Neighbor(S′) and all
             adjacent edges from G′.
        (d) I=I S′

      Check for |v| > 0. if there are |e| == 0, pick all the remaining v
     */
    var current_v_num = 0L//g_dash.vertices.count()
    var currnent_e_num =0L// g_dash.edges.count()
    /*
     * Use pregel to find independent motif
     * Step 1: update graph v to start with self node as MIS
     * Step 2: Each node send its motif string to each nbr
     * Step 3: when node receive a msg, it keeps the minimum of current MIS motif and incoming msg
     * Step 4: we pick Minimum V with found minimum motif id. (we could select maximum V als/o but then we may have
     * a situation (sometime, not always) a-b ==> MIS motif class is 'a 'but its MIS node to be returned is 'b'. so to
     * avoid, confusion,
      * we take
     * minimum V as the final MIS node to be returned)
     *
     */

    //NOTE: toGraphX first collects entire graphframe to master. Based on the Spark UI
    val g_dash_gx = g_dash.toGraphX.cache()
    val mis_attr_graph :Graph[(String,String),Row] = g_dash_gx.mapVertices((id,attr)
        => (attr.getAs[String](0),attr.getAs[String](0))).cache()


    //res_graph should have each vertex tagged with its MIS identified
    val res_graph  = mis_attr_graph.pregel[String]("Initial",1,EdgeDirection.Both)(
        (id,attr,newattr) => {
         /* if((attr._1.toString.contains
          ("1598_0_1542_1086316677|1542_0_44_1092891312|1598_0_44_1086318102")) ||
          attr._1.toString.contains("1598_0_44_1086318102|44_0_50_1082607289|1598_0_50_1086330186" +
                                    "") ||
            attr._1.toString.contains("1542_0_44_1092891312|44_0_456_1083571094|1542_0_456_1094017233"))
            println("again in motif")
          */
          // NOTE: ifjmax iteration is 1, it will run things in following order
          /*
           * 1. run vertex program with initial values
           * 2. run send msg
           * 3. run merge step
           * 4. run vertex program last time
           *
           * So vertex program is run maxItr + 1 times
           */

         if(newattr.equalsIgnoreCase("Initial")) (attr._1, attr._2)
            else
            {
              //2nd value on a vertex is it's canonical temporal motif. we are looking for
              // smallest possible canonical temporal motif
              if(attr._2.compareTo(newattr) <= 0)
                (attr._1,attr._2)
              else (attr._1,newattr)
            }
        }, //vertext progrm which is run at the start of each round including initial
        triplet => {
        /*  if( ((triplet.srcAttr._1.toString.contains(
          ("1598_0_44_1086318102|44_0_50_1082607289|1598_0_50_1086330186")))
            && (triplet.dstAttr._1.toString.contains(
          ("1598_0_1542_1086316677|1542_0_44_1092891312|1598_0_44_1086318102")))) ||
              ((triplet.dstAttr._1.toString.contains(
                                                      ("1598_0_44_1086318102|44_0_50_1082607289|1598_0_50_1086330186")))
               && (triplet.srcAttr._1.toString.contains(
                                                         ("1598_0_1542_1086316677|1542_0_44_1092891312|1598_0_44_1086318102")))) ||
              ((triplet.srcAttr._1.toString.contains(
                                                      ("1542_0_44_1092891312|44_0_456_1083571094|1542_0_456_1094017233")))
               && (triplet.dstAttr._1.toString.contains(
                                                         ("1598_0_1542_1086316677|1542_0_44_1092891312|1598_0_44_1086318102")))) )
            println("in motif")
          */
            if (triplet.srcAttr._2.compareTo(triplet.dstAttr._2) <= 0)
              Iterator((triplet.srcId, triplet.srcAttr._2),
                       (triplet.dstId, triplet.srcAttr._2)) // send src's MIS identifier to
            else
              Iterator((triplet.srcId, triplet.dstAttr._2),
                       (triplet.dstId, triplet.dstAttr._2)) // send dst's MIS identifier to
            // its nbr
            // nbr will pick the minimum one
        }
        ,
      (motifid1,motifid2) =>  {
          if (motifid1.compareTo(motifid2) <= 0) motifid1
          else motifid2
      }
      ).cache()

    // Now get Maximum vertex that corresponds to same MIS identifier
   /* val mis_id_set = res_graph.vertices.map(v => (v._2._2,v._2._1)).reduceByKey((v1,v2) => {
      if(v1.compareTo(v2) <= 0 ) v1
      else v2
    })*/

    val mis_id_set = res_graph.vertices.map(v => v._2._2).distinct()
    //res_graph.vertices.collect.foreach(v=> println("res v ", v._2._1,v._2._2))
    g_dash.unpersist(true)

    //because the 2nd element is the representative motif
    //var I : RDD[String] = mis_id_set.map(mis_v => mis_v).distinct()
   // mis_id_set.collect().foreach(mis => println(mis._1 + "=="+mis._2))
    //System.exit(1)


    //I
    //println(I.count())
    ///I
    mis_id_set
  }

}
