package com.frazier.bryce.vitalsigns

import org.apache.spark.rdd.RDD
import DiabetesConstants._

/**
  * Created by bryce frazier on 4/12/16.
  */
class DiabetesProcessor {
  def obtainDiabetesResults(records: RDD[HealthRecord]): RDD[(Long, HealthResult)] = {
    records.map { record =>
      val result = record.glucoseLvl match {
        case invalid if invalid <= 0 => throw new Exception("glucose lvl must be positive, with record: " + record.toString)
        case low if low < 45 => DIABETES_LOW_RESULT
        case desirable if desirable <= 99 => DIABETES_NORMAL_RESULT
        case prediabetes if prediabetes <= 125 => DIABETES_PRE_RESULT
        case _ => DIABETES_POSITIVE_RESULT
      }
      (record.id, result)
    }
  }
}

object DiabetesConstants {
  val DIABETES_KEY = "diabetes"
  val DIABETES_LOW_RESULT = HealthResult(DIABETES_KEY, "Abnormally low", "cautiously low")
  val DIABETES_NORMAL_RESULT = HealthResult(DIABETES_KEY, "Normal", "Stay healthy!")
  val DIABETES_PRE_RESULT = HealthResult(DIABETES_KEY, "Pre-Diabetes", "Borderline, maintain or adopt a healthy lifestyle!")
  val DIABETES_POSITIVE_RESULT = HealthResult(DIABETES_KEY, "Diabetes", "In the case it is positive, one needs to test again to be sure")
}

object DiabetesProcessor {
  def apply() = new DiabetesProcessor()
}