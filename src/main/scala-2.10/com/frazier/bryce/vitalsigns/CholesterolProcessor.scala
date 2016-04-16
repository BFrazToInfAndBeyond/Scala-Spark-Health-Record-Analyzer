package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.CholesterolConstants._
import org.apache.spark.rdd.RDD

/**
  * Created by bryce frazier on 4/11/16.
  */
class CholesterolProcessor {


  @throws[Exception]
  def obtainCholesterolResults(healthRecords: RDD[HealthRecord]): RDD[(Long, HealthResult)] = {
    healthRecords.flatMap { record =>
      val cholesterol = record.cholesterol
      if (cholesterol.hdl <= 0 || cholesterol.ldl <= 0 || cholesterol.total <= 0 || cholesterol.triglycerides <= 0) {
        throw new Exception("incorrect value(s) cholesterol with record: " + record.toString)
      }

      val totalCholesterolResult = cholesterol.total match {
        case desirable if desirable <= 200 => TOTAL_CLSTRL_DESIRABLE_RESULT
        case borderline if borderline < 240 => TOTAL_CLSTRL_BORDERLINE_RESULT
        case _ => TOTAL_CLSTRL_HIGH_RESULT
      }

      val ldlCholesterolResult = cholesterol.ldl match {
        case desirable if desirable <= 100 => LDL_CLSTRL_DESIRABLE_RESULT
        case nearDesirable if nearDesirable < 130 => LDL_CLSTRL_NEAR_DESIRABLE_RESULT
        case borderline if borderline < 160 => LDL_CLSTRL_BORDERLINE_RESULT
        case high if high < 190 => LDL_CLSTRL_HIGH_RESULT
        case _ => LDL_CLSTRL_VERY_HIGH_RESULT
      }

      val hdlCholesterolResult = cholesterol.hdl match {
        case lessRisk if lessRisk >= 60 => HDL_CLSTRL_LESS_RISK_RESULT
        case normal if normal > 40 => HDL_CLSTRL_NORMAL_RESULT
        case _ => HDL_CLSTRL_MAJOR_RISK_RESULT
      }

      val triglyceridesResult = cholesterol.triglycerides match {
        case desirable if desirable <= 150 => TRI_CLSTRL_DESIRABLE_RESULT
        case borderline if borderline < 200 => TRI_CLSTRL_BORDERLINE_RESULT
        case high if high < 500 => TRI_CLSTRL_HIGH_RESULT
        case _ => TRI_CLSTRL_VERY_HIGH_RESULT
      }

      List((record.id, totalCholesterolResult), (record.id, ldlCholesterolResult),
        (record.id, hdlCholesterolResult), (record.id, triglyceridesResult))
    }
  }
}

object CholesterolProcessor {
  def apply() = new CholesterolProcessor()
}

object CholesterolConstants {
  val TOTAL_CLSTRL_KEY = "total_cholesterol"
  val TOTAL_CLSTRL_DESIRABLE_RESULT = HealthResult(TOTAL_CLSTRL_KEY, "desirable", "stay healthy!")
  val TOTAL_CLSTRL_BORDERLINE_RESULT = HealthResult(TOTAL_CLSTRL_KEY, "borderline high", "Maintain or adopt a healthy lifestyle!")
  val TOTAL_CLSTRL_HIGH_RESULT = HealthResult(TOTAL_CLSTRL_KEY, "high", "Cautious")

  val LDL_CLSTRL_KEY = "ldl_cholesterol"
  val LDL_CLSTRL_DESIRABLE_RESULT = HealthResult(LDL_CLSTRL_KEY, "desirable", "stay healthy!")
  val LDL_CLSTRL_NEAR_DESIRABLE_RESULT = HealthResult(LDL_CLSTRL_KEY, "near desirable", "Maintain or adopt a healthy lifestyle!")
  val LDL_CLSTRL_BORDERLINE_RESULT = HealthResult(LDL_CLSTRL_KEY, "borderline high", "Maintain or adopt a healthy lifestyle!")
  val LDL_CLSTRL_HIGH_RESULT = HealthResult(LDL_CLSTRL_KEY, "high", "Cautious")
  val LDL_CLSTRL_VERY_HIGH_RESULT = HealthResult(LDL_CLSTRL_KEY, "very high", "Dangerously high")


  val HDL_CLSTRL_KEY = "hdl_cholesterol"
  val HDL_CLSTRL_LESS_RISK_RESULT = HealthResult(HDL_CLSTRL_KEY, "less risk", "stay healthy!")
  val HDL_CLSTRL_NORMAL_RESULT = HealthResult(HDL_CLSTRL_KEY, "normal", "stay healthy!")
  val HDL_CLSTRL_MAJOR_RISK_RESULT = HealthResult(HDL_CLSTRL_KEY, "major risk", "hdl cholesterol is very high!")


  val TRI_CLSTRL_KEY = "triglycerides"
  val TRI_CLSTRL_DESIRABLE_RESULT = HealthResult(TRI_CLSTRL_KEY, "desirable", "stay healthy!")
  val TRI_CLSTRL_BORDERLINE_RESULT = HealthResult(TRI_CLSTRL_KEY, "borderline high", "Maintain or adopt a healthy lifestyle!")
  val TRI_CLSTRL_HIGH_RESULT = HealthResult(TRI_CLSTRL_KEY, "high", "Cautious")
  val TRI_CLSTRL_VERY_HIGH_RESULT = HealthResult(TRI_CLSTRL_KEY, "very high", "dangerous. Note that a high triglyceride level has been linked to higher risk of coronary artery disease.")

}