package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.BloodPressureConstants._
import com.frazier.bryce.vitalsigns.CholesterolConstants._
import com.frazier.bryce.vitalsigns.DiabetesConstants._
import org.apache.spark.rdd.RDD

/**
  * Created by bryce frazier on 4/12/16.
  */
class Aggregator {

  def computeRatioOfDiabetesPatients(patientResults: RDD[PatientResults]): Float = {
    val distinctPatientResults = patientResults.map(result => result.patientId).distinct().count()
    val resultsWithDiabetes_groupedByPatientId = patientResults.flatMap {
      case diabetesPositive if diabetesPositive.healthResults.contains(DIABETES_POSITIVE_RESULT) =>
        Some((diabetesPositive.patientId, diabetesPositive))
      case _ => None
    }.groupByKey()

    // when a patient has a result indicating diabetes, a 2nd diabetes result is required before
    // declaring the patient has in fact diabetes.
    resultsWithDiabetes_groupedByPatientId match {
      case nothing if nothing.count() == 0 => 0
      case results => results.map {
        case (_, v) if v.toList.size >= 2 => 1
        case _ => 0
      }.reduce(_ + _).toFloat / distinctPatientResults
    }
  }

  def computeRatioOfHighBloodPressurePatients(patientResults: RDD[PatientResults]): Float = {
    computeRatioWithCriterion(patientResults, hasHighBloodPressure)
  }

  //A patient is considered to have high cholesterol results if he/she has high levels for all 4 cholesterol metrics
  def computeRatioOfHighCholesterolPatients(patientResults: RDD[PatientResults]) = {
    computeRatioWithCriterion(patientResults, hasHighCholesterolResults)
  }

  def computeRatioOfHealthyPatients(patientResults: RDD[PatientResults]): Float = {
    computeRatioWithCriterion(patientResults, isHealthy)
  }

  def obtainAggregatedResults(patientResults: RDD[PatientResults]): String = {
    val percentageOfDiabetesPatients = Math.round(computeRatioOfDiabetesPatients(patientResults) * 100)
    val percentageOfOfHealthyPatients = Math.round(computeRatioOfHealthyPatients(patientResults) * 100)
    val percentageOfHighBloodPressurePatients = Math.round(computeRatioOfHighBloodPressurePatients(patientResults) * 100)
    val percentageOfHighCholesterolPatients = Math.round(computeRatioOfHighCholesterolPatients(patientResults) * 100)

    val results = "******************************************\n" +
      s"Patients with High Blood Pressure: $percentageOfHighBloodPressurePatients%\n" +
      s"Patients with High Cholesterol: $percentageOfHighCholesterolPatients%\n" +
      s"Patients with Diabetes: $percentageOfDiabetesPatients%\n" +
      "******************************************\n" +
      s"Healthy Patients: $percentageOfOfHealthyPatients%\n" +
      "******************************************\n"
    results
  }

  private def computeRatioWithCriterion(patientResults: RDD[PatientResults], f: (List[HealthResult]) => Boolean): Float = {
    val latestPatientResults = getLatestPatientResults(patientResults)
    latestPatientResults.map { result =>
      if (f(result.healthResults)) 1
      else 0
    } match {
      case nothing if nothing.count() == 0 => 0
      case something => something.reduce((y: Int, x: Int) => y + x).toFloat / latestPatientResults.count()
    }
  }

  private def getLatestPatientResults(patientResults: RDD[PatientResults]): RDD[PatientResults] = {
    patientResults.map(result => (result.patientId, result)).groupByKey().map { case (_, v) => v.toList.sortBy(_.timestamp).last }
  }

  private val isHealthy: List[HealthResult] => Boolean = healthResults =>
    (healthResults.contains(DIABETES_LOW_RESULT) || healthResults.contains(DIABETES_NORMAL_RESULT)) &&
      (healthResults.contains(HDL_CLSTRL_LESS_RISK_RESULT) || healthResults.contains(HDL_CLSTRL_NORMAL_RESULT)) &&
      healthResults.contains(BP_NORMAL_RESULT) && healthResults.contains(TOTAL_CLSTRL_DESIRABLE_RESULT) &&
      healthResults.contains(LDL_CLSTRL_DESIRABLE_RESULT) && healthResults.contains(TRI_CLSTRL_DESIRABLE_RESULT)

  //A patient is considered to have high cholesterol results if he/she has high levels for all 4 cholesterol metrics
  private val hasHighCholesterolResults: List[HealthResult] => Boolean = healthResults =>
    (healthResults.contains(LDL_CLSTRL_HIGH_RESULT) || healthResults.contains(LDL_CLSTRL_VERY_HIGH_RESULT)) &&
      (healthResults.contains(TRI_CLSTRL_HIGH_RESULT) || healthResults.contains(TRI_CLSTRL_VERY_HIGH_RESULT)) &&
      healthResults.contains(HDL_CLSTRL_MAJOR_RISK_RESULT) && healthResults.contains(TOTAL_CLSTRL_HIGH_RESULT)

  private val hasHighBloodPressure: List[HealthResult] => Boolean = healthResults =>
    healthResults.exists {
      List(BP_STAGE_1_RESULT, BP_STAGE_2_RESULT, BP_HYPERSTENSIVE_CRISIS_RESULT).contains(_)
    }

}

object Aggregator {
  def apply() = new Aggregator()
}
