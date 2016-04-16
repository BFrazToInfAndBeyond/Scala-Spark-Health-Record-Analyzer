package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.CholesterolConstants._
import org.apache.spark.SparkContext
import org.scalatest._

/**
  * Created by bryce frazier on 4/11/16.
  */
class CholesterolProcessorSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {

  var testObj: CholesterolProcessor = _
  var sc: SparkContext = _
  val defaultCholesterol = Cholesterol(1, 1, 1, 1)

  before {
    testObj = CholesterolProcessor()
    sc = Spark.ctx
  }

  describe("A Cholesterol Processor") {
    describe("given any cholesterol values are not positive") {
      it("should throw error") {
        val invalidRecord = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(-1, 3, 2, 3), 60, "140/90")
        val record = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "145/70")
        val records = sc.parallelize(List(invalidRecord, record))
        intercept[Exception] {
          val actualResults = testObj.obtainCholesterolResults(records).collect
        }

        val invalidRecord2 = HealthRecord(3, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(4, 3, -1, 3), 60, "140/90")
        intercept[Exception] {
          val actualResults = testObj.obtainCholesterolResults(sc.parallelize(List(invalidRecord2, record))).collect
        }

        val invalidRecord3 = HealthRecord(4, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(4, 0, 0, 3), 60, "140/90")
        intercept[Exception] {
          val actualResults = testObj.obtainCholesterolResults(sc.parallelize(List(invalidRecord3))).collect
        }
      }
    }

    //      Cholesterol
    //      total
    //      # <= 200: Desirable
    //      200 < # < 240: Borderline High
    //      240 >= # : High
    describe("given total cholesterol") {
      it("should derive expected interpretations") {

        val desirable1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(200, 2, 3, 4), 60, "140/90")
        val desirable2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 2, 3, 4), 70, "145/70")
        val borderlineHigh1 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(201, 2, 3, 4), 77, "186/92")
        val borderlineHigh2 = HealthRecord(4, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(239, 2, 3, 4), 77, "186/92")
        val high = HealthRecord(5, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(240, 2, 3, 4), 77, "186/92")

        val records = sc.parallelize(List(desirable1, desirable2, borderlineHigh1, borderlineHigh2, high))

        val expectedResults = List(
          (1, TOTAL_CLSTRL_DESIRABLE_RESULT),
          (2, TOTAL_CLSTRL_DESIRABLE_RESULT),
          (3, TOTAL_CLSTRL_BORDERLINE_RESULT),
          (4, TOTAL_CLSTRL_BORDERLINE_RESULT),
          (5, TOTAL_CLSTRL_HIGH_RESULT))

        val actualResults = testObj.obtainCholesterolResults(records).collect
        actualResults.toList.filter(_._2.name == TOTAL_CLSTRL_KEY) should be(expectedResults)
      }
    }

    //      LDL Cholesterol
    //      # <= 100 : Desirable
    //      100 < # < 130 : Near Desirable
    //      130 <= # < 160 : Borderline High
    //      160 <= # < 190 : High
    //      190 >= # : Very High
    describe("given ldl cholesterol") {
      it("should derive expected interpretations") {
        val desirable = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(200, 100, 3, 4), 60, "140/90")
        val nearDesirable1 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 101, 3, 4), 70, "145/70")
        val nearDesirable2 = HealthRecord(3, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 129, 3, 4), 70, "145/70")
        val borderlineHigh1 = HealthRecord(4, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(201, 130, 3, 4), 77, "186/92")
        val borderlineHigh2 = HealthRecord(5, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(239, 159, 3, 4), 77, "186/92")
        val high1 = HealthRecord(6, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(240, 160, 3, 4), 77, "186/92")
        val high2 = HealthRecord(7, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(240, 189, 3, 4), 77, "186/92")
        val veryHigh = HealthRecord(8, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(240, 190, 3, 4), 77, "186/92")

        val records = sc.parallelize(List(desirable, nearDesirable1, nearDesirable2, borderlineHigh1, borderlineHigh2,
          high1, high2, veryHigh))

        val expectedResults = List(
          (1, LDL_CLSTRL_DESIRABLE_RESULT),
          (2, LDL_CLSTRL_NEAR_DESIRABLE_RESULT),
          (3, LDL_CLSTRL_NEAR_DESIRABLE_RESULT),
          (4, LDL_CLSTRL_BORDERLINE_RESULT),
          (5, LDL_CLSTRL_BORDERLINE_RESULT),
          (6, LDL_CLSTRL_HIGH_RESULT),
          (7, LDL_CLSTRL_HIGH_RESULT),
          (8, LDL_CLSTRL_VERY_HIGH_RESULT))

        val actualResults = testObj.obtainCholesterolResults(records).collect
        actualResults.toList.filter(_._2.name == LDL_CLSTRL_KEY) should be(expectedResults)
      }
    }

    //      HDL Cholesterol Level  ***
    //      # <= 40 : Major Risk
    //      40 < # < 60 : Normal  ** correct interp?
    //      60 >= # : Less Risk
    describe("given hdl cholesterol") {
      it("should derive expected interpretations") {
        val lessRisk = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(200, 100, 60, 4), 60, "140/90")
        val normal1 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 101, 41, 4), 70, "145/70")
        val normal2 = HealthRecord(3, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 129, 59, 4), 70, "145/70")
        val majorRisk = HealthRecord(4, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(201, 130, 40, 4), 77, "186/92")

        val records = sc.parallelize(List(lessRisk, normal1, normal2, majorRisk))

        val expectedResults = List(
          (1, HDL_CLSTRL_LESS_RISK_RESULT),
          (2, HDL_CLSTRL_NORMAL_RESULT),
          (3, HDL_CLSTRL_NORMAL_RESULT),
          (4, HDL_CLSTRL_MAJOR_RISK_RESULT))

        val actualResults = testObj.obtainCholesterolResults(records).collect
        actualResults.toList.filter(_._2.name == HDL_CLSTRL_KEY) should be(expectedResults)
      }
    }

    //      Triglycerides:
    //      # <= 150 : Desirable
    //      150 < # < 200 : Borderline high
    //      200 <= # < 500 : High
    //      500 >= # : Very High  A high triglyceride level has been linked to higher risk of coronary artery disease.
    describe("given triglyceride test results") {
      it("should derive expected interpretations") {
        val desirable = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(200, 100, 3, 150), 60, "140/90")
        val borderline1 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 101, 3, 151), 70, "145/70")
        val borderline2 = HealthRecord(3, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(199, 129, 3, 199), 70, "145/70")
        val high1 = HealthRecord(4, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(201, 130, 3, 200), 77, "186/92")
        val high2 = HealthRecord(5, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(239, 159, 3, 499), 77, "186/92")
        val veryHigh = HealthRecord(6, 3, 27, "M", "Northwest", 1326, 14, Cholesterol(240, 190, 3, 500), 77, "186/92")

        val records = sc.parallelize(List(desirable, borderline1, borderline2, high1, high2, veryHigh))

        val expectedResults = List(
          (1, TRI_CLSTRL_DESIRABLE_RESULT),
          (2, TRI_CLSTRL_BORDERLINE_RESULT),
          (3, TRI_CLSTRL_BORDERLINE_RESULT),
          (4, TRI_CLSTRL_HIGH_RESULT),
          (5, TRI_CLSTRL_HIGH_RESULT),
          (6, TRI_CLSTRL_VERY_HIGH_RESULT))

        val actualResults = testObj.obtainCholesterolResults(records).collect
        actualResults.toList.filter(_._2.name == TRI_CLSTRL_KEY) should be(expectedResults)
      }
    }


    describe("given cholesterol test results") {
      it("should derive expected interpretations") {
        val patient1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, Cholesterol(200, 100, 60, 150), 60, "140/90")
        val patient2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, Cholesterol(201, 200, 40, 200), 70, "145/70")
        val records = sc.parallelize(List(patient1, patient2))

        val totalExpectedResults = List(
          (1, TOTAL_CLSTRL_DESIRABLE_RESULT),
          (2, TOTAL_CLSTRL_BORDERLINE_RESULT))

        val ldlExpectedResults = List(
          (1, LDL_CLSTRL_DESIRABLE_RESULT),
          (2, LDL_CLSTRL_VERY_HIGH_RESULT))

        val hdlExpectedResults = List(
          (1, HDL_CLSTRL_LESS_RISK_RESULT),
          (2, HDL_CLSTRL_MAJOR_RISK_RESULT))

        val triExpectedResults = List(
          (1, TRI_CLSTRL_DESIRABLE_RESULT),
          (2, TRI_CLSTRL_HIGH_RESULT))

        val actualResults = testObj.obtainCholesterolResults(records).collect
        actualResults.toList.filter(_._2.name == TOTAL_CLSTRL_KEY) should be(totalExpectedResults)
        actualResults.toList.filter(_._2.name == LDL_CLSTRL_KEY) should be(ldlExpectedResults)
        actualResults.toList.filter(_._2.name == HDL_CLSTRL_KEY) should be(hdlExpectedResults)
        actualResults.toList.filter(_._2.name == TRI_CLSTRL_KEY) should be(triExpectedResults)
      }
    }
  }
}
