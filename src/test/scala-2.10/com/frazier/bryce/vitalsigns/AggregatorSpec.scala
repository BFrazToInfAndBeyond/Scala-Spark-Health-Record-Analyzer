package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.BloodPressureConstants._
import com.frazier.bryce.vitalsigns.CholesterolConstants._
import com.frazier.bryce.vitalsigns.DiabetesConstants._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.scalatest._

/**
  * Created by bryce frazier on 4/12/16.
  */
class AggregatorSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {

  var testObj: Aggregator = _
  var sc: SparkContext = _
  val defaultCholesterol = Cholesterol(1, 1, 1, 1)

  before {
    testObj = Aggregator()
    sc = Spark.ctx
  }

  describe("An Aggregator") {

    def ~=(x: Double, y: Double, precision: Double) = {
      if ((x - y).abs < precision) true else false
    }
    describe("given obtainAggregatedResults()") {
      describe("when called with patientResults") {
        it("should return computed figures") {

          val healthy1 = PatientResults(1, 1, 1324, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val patientResults: RDD[PatientResults] = sc.parallelize(List(healthy1))
          val expectedResults = "******************************************\n" +
            s"Patients with High Blood Pressure: 20%\n" +
            s"Patients with High Cholesterol: 88%\n" +
            s"Patients with Diabetes: 43%\n" +
            "******************************************\n" +
            s"Healthy Patients: 33%\n" +
            "******************************************\n"

          testObj = new Aggregator() {
            override def computeRatioOfHighBloodPressurePatients(patientResults: RDD[PatientResults]) = 0.2F

            override def computeRatioOfHighCholesterolPatients(patientResults: RDD[PatientResults]) = .877F

            override def computeRatioOfDiabetesPatients(patientResults: RDD[PatientResults]) = 0.43F

            override def computeRatioOfHealthyPatients(patientResults: RDD[PatientResults]) = 0.33333F
          }
          val actualResults = testObj.obtainAggregatedResults(patientResults)
          actualResults should be(expectedResults)
        }
      }
    }
    describe("given computeRatioOfHealthyPatients()") {
      describe("when no patient results as input") {
        it("should yield 0") {
          val patientResults: RDD[PatientResults] = sc.parallelize(List())
          val actualResults = testObj.computeRatioOfHealthyPatients(patientResults)
          actualResults should be(0)
        }
      }

      describe("when one record per patient") {
        it("should yield expected results") {
          val healthy1 = PatientResults(1, 1, 1324, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val healthy2 = PatientResults(2, 2, 1325, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val healthy3 = PatientResults(3, 3, 1324, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val healthy4 = PatientResults(4, 4, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val nonHealthy1 = PatientResults(5, 5, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonHealthy2 = PatientResults(6, 6, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val nonHealthy3 = PatientResults(7, 7, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_BORDERLINE_RESULT, DIABETES_LOW_RESULT))

          val patientResults = sc.parallelize(List(healthy1, nonHealthy2, healthy4, nonHealthy3, nonHealthy1, healthy2, healthy3))
          val actualResults = testObj.computeRatioOfHealthyPatients(patientResults)

          ~=(actualResults, 4.0 / 7, 0.0001) should be(true)
        }
      }

      describe("when there are multiple records per patient") {
        it("should use the last record for each patient") {
          val result1_healthy = PatientResults(1, 1, 1324, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val result1_not = PatientResults(1, 8, 1325, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val healthy2_healthy = PatientResults(2, 2, 1325, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val healthy2_not = PatientResults(2, 10, 1326, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val healthy3 = PatientResults(3, 3, 1324, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val nonHealthy1 = PatientResults(5, 5, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonHealthy2 = PatientResults(6, 6, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val nonHealthy3 = PatientResults(7, 7, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_BORDERLINE_RESULT, DIABETES_LOW_RESULT))

          val patientResults = sc.parallelize(List(healthy2_not, healthy2_healthy, nonHealthy2, result1_healthy, nonHealthy3, nonHealthy1, result1_not, healthy3))
          val actualResults = testObj.computeRatioOfHealthyPatients(patientResults)

          ~=(actualResults, 1.0 / 6, 0.0001) should be(true)
        }
      }

    }

    describe("given computeRatioOfHighBloodPressurePatients()") {
      // A Patient is considered to have high blood pressure if they have one of the following:
      //BP_STAGE_1_RESULT      OR      BP_STAGE_2_RESULT        OR       BP_HYPERSTENSIVE_CRISIS_RESULT

      describe("when no patient results as input") {
        it("should yield 0") {
          val patientResults: RDD[PatientResults] = sc.parallelize(List())
          val actualResults = testObj.computeRatioOfHighBloodPressurePatients(patientResults)
          actualResults should be(0)
        }
      }

      describe("when one record per patient") {
        it("should yield expected results") {
          val highBP1 = PatientResults(1, 1, 1324, List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val highBP2 = PatientResults(2, 2, 1325, List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val highBP3 = PatientResults(3, 3, 1324, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val highBP4 = PatientResults(4, 4, 1326, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val nonHighBP1 = PatientResults(5, 5, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonHighBP2 = PatientResults(6, 6, 1326, List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))

          val patientResults = sc.parallelize(List(highBP1, nonHighBP2, highBP4, highBP2, nonHighBP1, highBP3))
          val actualResults = testObj.computeRatioOfHighBloodPressurePatients(patientResults)

          ~=(actualResults, 4.0 / 6, 0.0001) should be(true)
        }
      }

      describe("when there are multiple records per patient") {
        it("should use the last record for each patient") {
          val result1_highBP1_old = PatientResults(1, 1, 1324, List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val result1_notHighBP1_new = PatientResults(1, 9, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val result2_highBP2_old = PatientResults(2, 2, 1325, List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val result2_notHighBP2_new = PatientResults(2, 3, 1328, List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val highBP4 = PatientResults(4, 4, 1326, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val nonHighBP1 = PatientResults(5, 5, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonHighBP2 = PatientResults(6, 6, 1326, List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))

          val patientResults = sc.parallelize(List(result1_highBP1_old, result1_notHighBP1_new, nonHighBP2, highBP4, nonHighBP1, result2_notHighBP2_new, result2_highBP2_old))
          val actualResults = testObj.computeRatioOfHighBloodPressurePatients(patientResults)

          ~=(actualResults, 1.0 / 5, 0.0001) should be(true)

        }
      }


    }
    describe("given computeRatioOfDiabetesPatients()") {

      describe("when no patient results as input") {
        it("should yield 0") {
          val patientResults: RDD[PatientResults] = sc.parallelize(List())
          val actualResults = testObj.computeRatioOfDiabetesPatients(patientResults)
          actualResults should be(0)
        }
      }

      describe("when one record per patient") {
        it("should yield 0, since a patient must have atleast 2 positive tests for diabetes to be labeled as so") {
          val diabetes = PatientResults(2, 3, 1328, List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonDiabetes1 = PatientResults(4, 4, 1326, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val nonDiabetes2 = PatientResults(5, 5, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_PRE_RESULT))
          val nonDiabetes3 = PatientResults(6, 6, 1326, List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))

          val patientResults = sc.parallelize(List(diabetes, nonDiabetes1, nonDiabetes2, nonDiabetes3))
          val actualResults = testObj.computeRatioOfDiabetesPatients(patientResults)

          actualResults should be(0)
        }
      }

      describe("when there are multiple records per patient") {
        it("should return as expected") {
          val diabetes_1_1 = PatientResults(1, 1, 1324, List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val diabetes_1_2 = PatientResults(1, 9, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonDiabetes_2 = PatientResults(2, 2, 1325, List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT))
          val nonDiabetes_3_1 = PatientResults(3, 3, 1328, List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val diabetes_4_1 = PatientResults(4, 4, 1326, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT))
          val nonDiabetes_4_2 = PatientResults(4, 5, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
          val nonDiabetes3_2 = PatientResults(3, 6, 1326, List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_PRE_RESULT))

          val patientResults = sc.parallelize(List(diabetes_4_1, nonDiabetes3_2, diabetes_1_1, nonDiabetes_2, nonDiabetes_3_1, diabetes_1_2, nonDiabetes_4_2))
          val actualResults = testObj.computeRatioOfDiabetesPatients(patientResults)
          ~=(actualResults, 1.0 / 4, 0.0001) should be(true)
        }
      }

    }

    describe("given computeRatioOfHighCholesterolPatients()") {

      describe("when no patient results as input") {
        it("should yield 0") {
          val patientResults: RDD[PatientResults] = sc.parallelize(List())
          val actualResults = testObj.computeRatioOfHighCholesterolPatients(patientResults)
          actualResults should be(0)
        }
      }

      describe("when one record per patient") {
        it("should yield expected results") {
          val highCol1 = PatientResults(1, 1, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val highCol2 = PatientResults(2, 2, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT))
          val highCol3 = PatientResults(3, 3, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val highCol4 = PatientResults(4, 4, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT))

          val non1 = PatientResults(5, 5, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val non2 = PatientResults(6, 6, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT))
          val non3 = PatientResults(7, 7, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val non4 = PatientResults(8, 8, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val non5 = PatientResults(9, 9, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT))
          val non6 = PatientResults(10, 10, 1324, List(TOTAL_CLSTRL_BORDERLINE_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val non7 = PatientResults(11, 11, 1324, List(TOTAL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))

          val patientResults = sc.parallelize(List(non1, non2, non7, highCol4, non5, highCol3, highCol1, highCol2, non6, non3, non4))
          val actualResults = testObj.computeRatioOfHighCholesterolPatients(patientResults)
          ~=(actualResults, 4.0 / 11, 0.0001) should be(true)
        }
      }

      describe("when there are multiple records per patient") {
        it("should use the last record for each patient") {
          val patient1_highCol_old = PatientResults(1, 1, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val patient1_non = PatientResults(1, 2, 1325, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT))
          val patient2_non = PatientResults(2, 3, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT))
          val patient3_non = PatientResults(3, 4, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT))

          val patient4_highCol_old = PatientResults(4, 5, 1324, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT))
          val patient4_non = PatientResults(4, 6, 1325, List(TOTAL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT))

          val patientResults = sc.parallelize(List(patient3_non, patient1_highCol_old, patient4_non, patient1_non, patient2_non, patient4_highCol_old))
          val actualResults = testObj.computeRatioOfHighCholesterolPatients(patientResults)
          actualResults should be(0)
        }
      }

    }
  }
}
