package com.sg

//Program2-spark

import scala.io.Source

case class AadharAuthInfo(service_agency: String, aua: String, res_state_name: String)

object AadharSparkProcessor {

  def readFile(path: String) = Source.fromFile(path)

  val get_aadhar_auth_info = (line: String) => {
    val columns: Array[String] = line.split(""",""")
    AadharAuthInfo(columns(3), columns(2), columns(128))
  }

  def main(args: Array[String]): Unit = {
    val aadharInput = args(0)
    val aadhar_data = readFile(aadharInput)

    val auth_data = aadhar_data.getLines().map(get_aadhar_auth_info).filter(f => f.service_agency.matches("""\d+""") && !f.res_state_name.equalsIgnoreCase("Delhi") && f.aua.matches("""\d+"""))
      .filter(_.aua.toLong > 650000)
    auth_data.foreach(println)

  }
}
