package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.BloodPressureConstants._
import org.apache.spark.SparkContext
import org.scalatest._

/**
  * Created by bryce frazier on 4/11/16.
  */
class BloodPressureProcessorSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {

  var testObj: BloodPressureProcessor = _
  var sc: SparkContext = _
  val defaultCholesterol = Cholesterol(1, 1, 1, 1)

  before {
    testObj = BloodPressureProcessor()
    sc = Spark.ctx
  }

  describe("A BloodPressureProcessor") {

    //      test scenarios:
    //       parsable input in the correct form (exact size of 2 and positive integers)
    //
    //        http://www.heart.org/HEARTORG/Conditions/HighBloodPressure/AboutHighBloodPressure/Understanding-Blood-Pressure-Readings_UCM_301764_Article.jsp#.Vwq8e1xVikp
    //          categories defined by the American Heart Association.
    //           Blood pressure            Systolic                Diastolic
    //           category                 mm Hg (upper #)           mm Hg (lower #)
    //          1) Normal                 less than 120     AND    less than 80
    //         2) Prehypertension             120 – 139      OR     80 – 89
    //          3)High Blood Pressure       140 – 159       OR      90 – 99
    //          (Hypertension) Stage 1
    //
    //          4)High Blood Pressure     160 or higher       OR      100 or higher
    //          (Hypertension) Stage 2
    //
    //          5)Hypertensive Crisis       Higher than 180     OR    Higher than 110
    //            (Emergency care needed)
    //

    describe("given blood pressure malformed input") {
      describe("where numerator and denom can not be parsed as int") {
        it("should throw error") {
          val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "119/79sdf")
          val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "120/70")

          val records = sc.parallelize(List(healthRecord1, healthRecord2))

          intercept[Exception] {
            val actualResults = testObj.obtainBloodPressureResults(records).collect
          }
        }
      }

      describe("where there exist negative values") {
        it("should throw error") {
          val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "-119/79")
          val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "120/70")

          val records = sc.parallelize(List(healthRecord1, healthRecord2))

          intercept[Exception] {
            val actualResults = testObj.obtainBloodPressureResults(records).collect
          }

        }
      }

      describe("where there does not exists a numerator or denominator") {
        it("should throw error") {
          val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "2/")
          val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "120/70")

          val records = sc.parallelize(List(healthRecord1, healthRecord2))

          intercept[Exception] {
            val actualResults = testObj.obtainBloodPressureResults(records).collect
          }
        }
      }
    }

    //    1) Systolic: less than 120     AND    Diastolic: less than 80
    describe("given Normal blood pressure") {
      it("should declare correct interpretation") {
        val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "119/79")
        val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "120/70")
        val healthRecord3 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "119/80")
        val healthRecord4 = HealthRecord(4, 4, 28, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "121/81")
        val healthRecord5 = HealthRecord(5, 5, 29, "M", "Southeast", 1329, 20, defaultCholesterol, 60, "110/40")

        val records = sc.parallelize(List(healthRecord1, healthRecord2, healthRecord3, healthRecord4, healthRecord5))

        val expectedResults =
          List((1, BP_NORMAL_RESULT),
            (2, BP_PREHYPERTENSION_RESULT),
            (3, BP_PREHYPERTENSION_RESULT),
            (4, BP_PREHYPERTENSION_RESULT),
            (5, BP_NORMAL_RESULT))

        val actualResults = testObj.obtainBloodPressureResults(records)

        actualResults.collect should be(expectedResults)
      }
    }

    //    2) ( S < 140 AND D < 90 )   AND   (S >= 120 OR D >= 80)
    describe("given Prehypertension") {
      it("should declare expected interpretation") {
        val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "120/79")
        val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "130/93")
        val healthRecord3 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "119/80")
        val healthRecord4 = HealthRecord(4, 4, 28, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "145/89")
        val healthRecord5 = HealthRecord(5, 5, 29, "M", "Southeast", 1329, 20, defaultCholesterol, 60, "110/90")

        val records = sc.parallelize(List(healthRecord1, healthRecord2, healthRecord3, healthRecord4, healthRecord5))

        val expectedResults =
          List((1, BP_PREHYPERTENSION_RESULT),
            (2, BP_STAGE_1_RESULT),
            (3, BP_PREHYPERTENSION_RESULT),
            (4, BP_STAGE_1_RESULT),
            (5, BP_STAGE_1_RESULT))

        val actualResults = testObj.obtainBloodPressureResults(records)

        actualResults.collect.toList should be(expectedResults)
      }
    }

    //    3)  (140 <= S < 160    AND  D < 100)     OR     (S < 160  AND  90 <= D < 100)
    describe("given High Blood Pressure - stage 1") {
      it("should declare expected interpretation") {

        val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "140/89")
        val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "139/90")
        val healthRecord3 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "140/90")
        val healthRecord4 = HealthRecord(4, 4, 28, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "160/99")
        val healthRecord5 = HealthRecord(5, 5, 29, "M", "Southeast", 1329, 20, defaultCholesterol, 60, "110/90")
        val healthRecord6 = HealthRecord(6, 6, 29, "M", "Southeast", 1329, 20, defaultCholesterol, 60, "159/100")

        val records = sc.parallelize(List(healthRecord1, healthRecord2, healthRecord3, healthRecord4, healthRecord5, healthRecord6))

        val expectedResults =
          List((1, BP_STAGE_1_RESULT),
            (2, BP_STAGE_1_RESULT),
            (3, BP_STAGE_1_RESULT),
            (4, BP_STAGE_2_RESULT),
            (5, BP_STAGE_1_RESULT),
            (6, BP_STAGE_2_RESULT))

        val actualResults = testObj.obtainBloodPressureResults(records)

        actualResults.collect.toList should be(expectedResults)

      }
    }

    //    4)  (160 <= S < 180  AND D < 110)   OR    (S < 180  AND  100 <= D < 110)
    describe("given High Blood Pressure - stage 2") {
      it("should declare expected interpretation") {
        val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "160/99")
        val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "158/109")
        val healthRecord3 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "170/110")
        val healthRecord4 = HealthRecord(4, 4, 28, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "180/100")
        val healthRecord5 = HealthRecord(5, 5, 29, "M", "Southeast", 1329, 20, defaultCholesterol, 60, "170/108")

        val records = sc.parallelize(List(healthRecord1, healthRecord2, healthRecord3, healthRecord4, healthRecord5))

        val expectedResults =
          List((1, BP_STAGE_2_RESULT),
            (2, BP_STAGE_2_RESULT),
            (3, BP_HYPERSTENSIVE_CRISIS_RESULT),
            (4, BP_HYPERSTENSIVE_CRISIS_RESULT),
            (5, BP_STAGE_2_RESULT))

        val actualResults = testObj.obtainBloodPressureResults(records)

        actualResults.collect.toList should be(expectedResults)

      }
    }

    //    5) Systolic:  >= 180     OR    Diastolic: >= 110
    describe("given Hypertensive Crisis") {
      it("should declare expected interpretation") {
        val healthRecord1 = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 20, defaultCholesterol, 60, "180/109")
        val healthRecord2 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "180/110")
        val healthRecord3 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 14, defaultCholesterol, 77, "200/180")

        val records = sc.parallelize(List(healthRecord1, healthRecord2, healthRecord3))

        val expectedResults =
          List((1, BP_HYPERSTENSIVE_CRISIS_RESULT),
            (2, BP_HYPERSTENSIVE_CRISIS_RESULT),
            (3, BP_HYPERSTENSIVE_CRISIS_RESULT))

        val actualResults = testObj.obtainBloodPressureResults(records)

        actualResults.collect.toList should be(expectedResults)

      }
    }
  }
}
