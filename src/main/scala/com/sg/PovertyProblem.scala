package com.sg

import scala.io.Source


case class PovertyEstimates(state_abr_code : String, area_name: String, urban_influence_code_2003: String, rural_urban_continuum_code_2013: String, POV_all_2018: String , POV_0_17_2018: String)
case class PovertyReport(state_name: String, area_name: String, urban_influence_code_2003: String, rural_urban_continuum_code_2013: String , pov_elder_than17_2018 : Double )

object PovertyProblem {

  def readFile ( path: String ) = Source.fromFile(path)

  val get_poverty_estimates = (line : String ) => {
    val columns:Array[String] =  line.split(""":""")
    PovertyEstimates(columns(1),columns(2),columns(4),columns(5),columns(7),columns(13))
  }

  val get_state_capitals = (states: List[String]) => {
    var map = Map[String, String]()
    states.foreach { state =>
      val columns: Array[String] = state.split(""",""")
      map += (columns(1).toLowerCase() -> columns(0))
    }
    map
  }

  def main(args: Array[String]): Unit = {
    val povertyInput = args(0)
    val statesInput = args(1)
    val poverty_estimates = readFile(povertyInput).getLines().drop(1).map(get_poverty_estimates)
      .filter(f => !f.urban_influence_code_2003.isEmpty() && !f.rural_urban_continuum_code_2013.isEmpty())
      .filter(f => f.urban_influence_code_2003.toLong % 2 != 0  && f.rural_urban_continuum_code_2013.toLong % 2 == 0)

    val state_names_data = readFile(statesInput).getLines().drop(1).toList
    val state_names = get_state_capitals(state_names_data)

    poverty_estimates.map {x =>
      val state = state_names.get(x.state_abr_code.toLowerCase()).getOrElse("")
      val area_name = s"${x.area_name} ${x.state_abr_code}"
      val pov_elder_17 : Double = (x.POV_0_17_2018.replaceAll(",", "").trim().toDouble / x.POV_all_2018.replaceAll(",", "").trim().toDouble ) * 100
      PovertyReport(state , area_name , x.urban_influence_code_2003 , x.rural_urban_continuum_code_2013 ,  pov_elder_17)
    }.foreach(println)

  }
}
