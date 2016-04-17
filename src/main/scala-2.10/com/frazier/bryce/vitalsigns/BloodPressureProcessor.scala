package com.frazier.bryce.vitalsigns

import org.apache.spark.rdd.RDD
import BloodPressureConstants._

/**
  * Created by bryce frazier on 4/11/16.
  */
class BloodPressureProcessor {

  @throws[Exception]
  def obtainBloodPressureResults(healthRecords: RDD[HealthRecord]): RDD[(Long, HealthResult)] = {
    healthRecords.map { record =>
      record.bloodPressure.split("/") match {
        case Array(systolic, diastolic) =>
          try {
            val s = systolic.toInt
            val d = diastolic.toInt
            if (s <= 0 || d <= 0) throw new Exception()
            else if ( s >= 180 ||  d >= 110) (record.id, BP_HYPERSTENSIVE_CRISIS_RESULT)
            else if (s >= 160  || d >= 100) (record.id, BP_STAGE_2_RESULT)
            else if (s >= 140 || d >= 90) (record.id, BP_STAGE_1_RESULT)
            else if  (s >= 120 || d >= 80) (record.id, BP_PREHYPERTENSION_RESULT)
            else (record.id, BP_NORMAL_RESULT) // (s < 120 && d < 80)
          } catch {
            case e: Exception => throw new Exception("incorrect blood pressure format with record: " + record.toString)
          }
        case _ => throw new Exception("incorrect blood pressure format with record: " + record.toString)
      }
    }
  }
}


object BloodPressureConstants {
  def getInterpretation(number: Int) = {
    s"$number/5, from a scale of 1: Normal TO 5: See a doctor immediately"
  }

  val BP_KEY = "blood_pressure"
  val BP_NORMAL_RESULT = HealthResult(BP_KEY, "Normal", "Stay healthy!")
  val BP_PREHYPERTENSION_RESULT = HealthResult(BP_KEY, "Prehypertension", "Maintain or adopt a healthy lifestyle!")
  val BP_STAGE_1_RESULT = HealthResult(BP_KEY, "High Blood Pressure - stage 1", getInterpretation(3))
  val BP_STAGE_2_RESULT = HealthResult(BP_KEY, "High Blood Pressure - stage 2", getInterpretation(4))
  val BP_HYPERSTENSIVE_CRISIS_RESULT = HealthResult(BP_KEY, "Hypertensive Crisis", getInterpretation(5))

}

object BloodPressureProcessor {
  def apply() = new BloodPressureProcessor()
}