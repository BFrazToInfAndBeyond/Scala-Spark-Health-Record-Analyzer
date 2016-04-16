package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.DiabetesConstants._
import com.frazier.bryce.vitalsigns.BloodPressureConstants._
import com.frazier.bryce.vitalsigns.CholesterolConstants._
import org.apache.spark.SparkContext
import org.scalatest._

/**
  * Created by bryce frazier on 4/10/16.
  */
class TransformerSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {

  var testObj: Transformer = _
  var sc: SparkContext = _
  val defaultCholesterol = Cholesterol(1, 1, 1, 1)

  before {
    testObj = new Transformer()
    sc = Spark.ctx
  }

  describe("A Transformer") {
    describe("given groupByPatientId_sortedByTimestamp()") {
      describe("when there are various records from various patients") {
        it("should group the records by patientId and sort by timestamp") {

          val healthRecord0_1 = HealthRecord(1, 0, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "140/90")
          val healthRecord0_2 = HealthRecord(2, 0, 20, "M", "Southeast", 1325, 20, defaultCholesterol, 60, "140/90")
          val healthRecord0_3 = HealthRecord(5, 0, 20, "M", "Southeast", 1326, 20, defaultCholesterol, 60, "140/90")
          val healthrecord1_1 = HealthRecord(3, 1, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "145/70")
          val healthrecord1_2 = HealthRecord(4, 1, 25, "F", "Northwest", 1326, 10, defaultCholesterol, 70, "145/70")
          val healthrecord2_1 = HealthRecord(6, 2, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "186/92")

          val recordsOutOfOrder = sc.parallelize(List(healthrecord1_2, healthrecord2_1, healthRecord0_3, healthrecord1_1, healthRecord0_2, healthRecord0_1))

          val expectedResults = List((0, List(healthRecord0_1, healthRecord0_2, healthRecord0_3)), (1, List(healthrecord1_1, healthrecord1_2)), (2, List(healthrecord2_1)))

          val actualResults = testObj.groupByPatientId_sortedByTimestamp(recordsOutOfOrder)

          actualResults.collect should be(expectedResults)
        }
      }
    }

    describe("given groupByPatientId__latestRecord()") {
      describe("when there are record(s) from various patients") {
        it("should group return the latest record per patient") {

          val healthRecord0_1 = HealthRecord(1, 0, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "140/90")
          val healthRecord0_2 = HealthRecord(2, 0, 20, "M", "Southeast", 1325, 20, defaultCholesterol, 60, "140/90")
          val healthRecord0_3 = HealthRecord(5, 0, 20, "M", "Southeast", 1326, 20, defaultCholesterol, 60, "140/90")
          val healthrecord1_1 = HealthRecord(3, 1, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "145/70")
          val healthrecord1_2 = HealthRecord(4, 1, 25, "F", "Northwest", 1326, 10, defaultCholesterol, 70, "145/70")
          val healthrecord2_1 = HealthRecord(6, 2, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "186/92")

          val recordsOutOfOrder = sc.parallelize(List(healthrecord1_2, healthrecord2_1, healthRecord0_3, healthrecord1_1, healthRecord0_2, healthRecord0_1))

          val expectedResults = List((0, healthRecord0_3), (1, healthrecord1_2), (2, healthrecord2_1))

          val actualResults = testObj.groupByPatientId_latestRecord(recordsOutOfOrder)

          actualResults.collect should be(expectedResults)
        }
      }
    }

    describe("given groupHealthResultsByRecordId()") {
      it("should group as expected") {
        val healthResults = List((1L, BP_STAGE_2_RESULT), (1L, TOTAL_CLSTRL_DESIRABLE_RESULT), (1L, LDL_CLSTRL_DESIRABLE_RESULT), (1L, HDL_CLSTRL_LESS_RISK_RESULT), (1L, TRI_CLSTRL_DESIRABLE_RESULT), (1L, DIABETES_LOW_RESULT),
          (2L, BP_HYPERSTENSIVE_CRISIS_RESULT), (2L, TOTAL_CLSTRL_HIGH_RESULT), (2L, LDL_CLSTRL_VERY_HIGH_RESULT), (2L, HDL_CLSTRL_MAJOR_RISK_RESULT), (2L, TRI_CLSTRL_VERY_HIGH_RESULT), (2L, DIABETES_POSITIVE_RESULT))

        val expectedResults = List(
          (1L, List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT)),
          (2L, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, DIABETES_POSITIVE_RESULT)))

        val healthResultsRDD = sc.parallelize(healthResults)
        val actualResults = testObj.groupHealthResultsByRecordId(healthResultsRDD)
        actualResults.collect should be(expectedResults)
      }
    }

    describe("given obtainPatientResults() called with health records and health results by record Id") {
      it("should return expected patient results") {
        val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 44, Cholesterol(200, 100, 60, 150), 60, "160/100")
        val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 130, Cholesterol(240, 190, 40, 500), 70, "200/180")
        val patient1 = PatientResults(1, 1, 1324, List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT))
        val patient2 = PatientResults(2, 2, 1325, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, DIABETES_POSITIVE_RESULT))

        val expectedResults = List(patient1, patient2)

        val healthResultsByRecordId = sc.parallelize(List(
          (1L, List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT)),
          (2L, List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, DIABETES_POSITIVE_RESULT)))
        )
        val healthRecords = sc.parallelize(List(healthRecord1, healthRecord2))

        val actualResults = testObj.obtainPatientResults(healthResultsByRecordId, healthRecords)

        actualResults.collect should be(expectedResults)
      }
    }

    describe("given groupByRegion()") {
      describe("when there are various records from various regions") {
        it("should group as expected") {

          val healthRecord0 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "140/90")
          val healthrecord1 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "145/70")
          val healthrecord2 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "186/92")

          val records = sc.parallelize(List(healthRecord0, healthrecord1, healthrecord2))

          val expectedResults = List(("Northwest", List(healthrecord1, healthrecord2)),("Southeast", List(healthRecord0)))

          val actualResults = testObj.groupByRegion(records)

          actualResults.collect.map { case (k, v) => (k, v.toList) } should be(expectedResults)
        }
      }
    }
  }
}

