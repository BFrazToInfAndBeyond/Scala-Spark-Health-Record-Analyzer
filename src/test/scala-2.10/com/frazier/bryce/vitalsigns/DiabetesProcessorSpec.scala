package com.frazier.bryce.vitalsigns

import com.frazier.bryce.vitalsigns.DiabetesConstants._
import org.apache.spark.SparkContext
import org.scalatest._

/**
  * Created by bryce frazier on 4/12/16.
  */
class DiabetesProcessorSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {

  var testObj: DiabetesProcessor = _
  var sc: SparkContext = _
  val defaultCholesterol = Cholesterol(1, 1, 1, 1)

  before {
    testObj = DiabetesProcessor()
    sc = Spark.ctx
  }

  describe("A DiabetesProcessor") {
    describe("given glucose malformed input") {

      describe("given any glucose values are not positive") {
        it("should throw error") {
          val invalidRecord = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 0, Cholesterol(-1, 3, 2, 3), 60, "140/90")
          val record = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 10, defaultCholesterol, 70, "145/70")
          val records = sc.parallelize(List(invalidRecord, record))
          intercept[Exception] {
            val actualResults = testObj.obtainDiabetesResults(records).collect
          }

          val invalidRecord2 = HealthRecord(3, 1, 20, "M", "Southeast", 1324, -1, Cholesterol(4, 3, -1, 3), 60, "140/90")
          intercept[Exception] {
            val actualResults = testObj.obtainDiabetesResults(sc.parallelize(List(invalidRecord2))).collect
          }
        }
      }


      //  # < 45 => Abnormally low
      //  from  45 to 99 mg/DL (~3 to 5.5 mmol/L)  	Normal fasting glucose  //http://www.healthieryou.com/hypo.html
      //    from 100 to 125 mg/DL(5.6 to 6.9 mmol/L)   	Prediabetes (impaired fasting glucose)
      //  from 126 mg/DL(7.0 mmol/L) 				  Diabetes   (if only one => declare need to take another to be more decisive)
      describe("given glucose levels") {
        it("should declare correct interpretation") {
          val low = HealthRecord(1, 1, 20, "M", "Southeast", 1324, 44, defaultCholesterol, 60, "119/79")
          val normal1 = HealthRecord(2, 2, 25, "F", "Northwest", 1325, 45, defaultCholesterol, 70, "120/70")
          val normal2 = HealthRecord(3, 3, 27, "M", "Northwest", 1326, 99, defaultCholesterol, 77, "119/80")
          val prediabetes1 = HealthRecord(4, 4, 28, "M", "Northwest", 1326, 100, defaultCholesterol, 77, "121/81")
          val prediabetes2 = HealthRecord(5, 5, 29, "M", "Southeast", 1329, 125, defaultCholesterol, 60, "110/40")
          val diabetes = HealthRecord(6, 5, 29, "M", "Southeast", 1329, 126, defaultCholesterol, 60, "110/40")

          val records = sc.parallelize(List(low, normal1, normal2, prediabetes1, prediabetes2, diabetes))

          val expectedResults =
            List((1, DIABETES_LOW_RESULT),
              (2, DIABETES_NORMAL_RESULT),
              (3, DIABETES_NORMAL_RESULT),
              (4, DIABETES_PRE_RESULT),
              (5, DIABETES_PRE_RESULT),
              (6, DIABETES_POSITIVE_RESULT))

          val actualResults = testObj.obtainDiabetesResults(records)

          actualResults.collect should be(expectedResults)
        }
      }
    }
  }
}
