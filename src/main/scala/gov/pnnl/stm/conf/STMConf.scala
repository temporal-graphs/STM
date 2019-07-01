package gov.pnnl.stm.conf

object STMConf {
  val atomocMotif: Map[String, String] =
    Map(
      "isolatednode" -> "a",
      "isolatededge" -> "(a)-[e1]->(b)",
      "multiedge" -> "(a)-[e1]->(b); (a)-[e2]->(b)",
      "selfloop" -> "(a)-[e1]->(b)",
      "triangle" -> "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)",
      "triad" -> "(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(c)",
      "twoloop" -> "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(b); (b)-[e4]->(a)",
      "quad" -> "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)",
      "loop" -> "(a)-[e1]->(b); (b)-[e2]->(a)",
      "outstar" -> "(a)-[e1]->(b); (a)-[e2]->(c); (a)-[e3]->(d)",
      "instar" -> "(b)-[e1]->(a); (c)-[e2]->(a); (d)-[e3]->(a)",
      "outdiad" -> "(a)-[e1]->(b); (a)-[e2]->(c)",
      "indiad" -> "(b)-[e1]->(a); (c)-[e2]->(a)",
      "inoutdiad" -> "(a)-[e1]->(b); (b)-[e2]->(c)",
      "residualedge" -> "(a)-[e1]->(b)"
    )

  val atomocMotifKeyToName: Map[Int, String] =
    Map(
      0 -> "simulatanious",
      1 -> "isolatednode",
      2 -> "isolatededge",
      3 -> "multiedge",
      4 -> "selfloop",
      5 -> "triangle",
      6 -> "triad",
      7 -> "twoloop",
      8 -> "quad",
      9 -> "loop",
      10 -> "outstar",
      11 -> "instar",
      12 -> "outdiad",
      13 -> "indiad",
      14 -> "inoutdiad",
      15 -> "residualedge"
    )
  val atomocMotifNameToKey: Map[String, Int] =
    for ((k, v) <- atomocMotifKeyToName) yield (v, k)

  val motifNameToOrbitKeys: Map[String,Map[Int,Int]] =
  Map(
    "simulatanious" -> Map(1->0,2->1),
    "isolatednode" -> Map(1->2),
    "isolatededge" -> Map(1->3,2->4),
    "multiedge"-> Map(1->5,2->6),
    "selfloop"-> Map(1->7),
    "triangle" -> Map(1->8),
    "triad" -> Map(1->9,2->10,3->11),
    "twoloop"-> Map(1->12,2->13),
    "quad"-> Map(1->14),
    "loop" -> Map(1->15),
    "outstar" -> Map(1->16,2->17),
    "instar" -> Map(1->18,2->19),
    "outdiad" -> Map(1->20,2->21),
    "indiad" -> Map(1->22,2->23),
    "inoutdiad" -> Map(1->24,2->25,3->26),
    "residualedge" -> Map(1->27,2->28)
  )
}
