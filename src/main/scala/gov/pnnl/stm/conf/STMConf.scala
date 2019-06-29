package gov.pnnl.stm.conf

object STMConf {
  val atomocMotif: Map[String, String] =
    Map(
      "triangle" -> "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)",
      "triad" -> "(a)-[e1]->(b); (b)-[e2]->(c); (a)-[e3]->(c)",
      "outdiad" -> "(a)-[e1]->(b); (a)-[e2]->(c)",
      "indiad" -> "(b)-[e1]->(a); (c)-[e2]->(a)",
      "loop" -> "(a)-[e1]->(b); (b)-[e2]->(a)",
      "residualedge" -> "(a)-[e1]->(b)",
      "selfloop" -> "(a)-[e1]->(b)",
      "multiedge" -> "(a)-[e1]->(b); (a)-[e2]->(b)",
      "isolatednode" -> "a",
      "isolatededge" -> "(a)-[e1]->(b)",
      "quad" -> "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)",
      "outstar" -> "(a)-[e1]->(b); (a)-[e2]->(c); (a)-[e3]->(d)",
      "instar" -> "(b)-[e1]->(a); (c)-[e2]->(a); (d)-[e3]->(a)",
      "twoloop" -> "(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(b); (b)-[e4]->(a)",
      "inoutdiad" -> "(a)-[e1]->(b); (b)-[e2]->(c)"
    )

    val atomocMotifKey: Map[Int, String] =
      Map(
        5 -> "triangle",
        6 -> "triad",
        12 -> "outdiad",
        13 -> "indiad",
        9 -> "loop",
        15 -> "residualedge",
        4 -> "selfloop",
        3 -> "multiedge",
        1 -> "isolatednode",
        2 -> "isolatededge",
        8 -> "quad",
        10 -> "outstar",
        11 -> "instar",
        7 -> "twoloop",
        14 -> "inoutdiad"
      )
}
