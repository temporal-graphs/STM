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

  val motifNameToITeMKeys: Map[String,Map[Int,Int]] =
    Map(
      "simulatanious" -> Map(0->0,1->1,2->2),
      "isolatednode" -> Map(0->3),
      "isolatededge" -> Map(0->4),
      "multiedge"-> Map(2->5),
      "selfloop"-> Map(0->6,1->7),
      "triangle" -> Map(0->8,1->9,2->10,3->11),
      "triad" -> Map(0->12,1->13,2->14,3->15),
      "twoloop"-> Map(0->16,1->17,2->18,3->19),
      "quad"-> Map(0->20,1->21,2->22,3->23,4->24),
      "loop" -> Map(0->25,1->26,2->27),
      "outstar" -> Map(0->28,1->29,2->30,3->31,4->32),
      "instar" -> Map(0->33,1->34,2->35,3->36,4->37),
      "outdiad" -> Map(0->38,1->39,2->40,3->41),
      "indiad" -> Map(0->42,1->43,2->44,3->45),
      "inoutdiad" -> Map(0->46,1->47,2->48,3->49),
      "residualedge" -> Map(0->50,1->51)
    )
}
