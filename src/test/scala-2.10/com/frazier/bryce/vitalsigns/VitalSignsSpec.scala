package com.frazier.bryce.vitalsigns

import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.scalatest._
import org.apache.hadoop.fs.{Path, FileSystem}
import com.frazier.bryce.vitalsigns.BloodPressureConstants._
import com.frazier.bryce.vitalsigns.CholesterolConstants._
import com.frazier.bryce.vitalsigns.DiabetesConstants._
/**
  * Created by bryce frazier on 4/16/16.
  */
class VitalSignsSpec extends FunSpec with BeforeAndAfter with ShouldMatchers {

  var testObj: VitalSigns = _
  var sc: SparkContext = _

  val emptyFileOfHealthRecords = "src/test/resources/empty_file_of_health_records.txt"
  val invalidHealthRecords = "src/test/resources/invalid_health_records.txt"
  val validHealthRecords = "src/test/resources/sample_health_records.txt"
  val outputDir = "src/test/resources/sample_output"
  val outputPath = new Path(outputDir)
  val fs = FileSystem.get(new Configuration)

  before {
    fs.delete(outputPath, true)
    testObj = VitalSigns()
    sc = Spark.ctx
  }

  after {
    fs.delete(outputPath, true)
  }

  describe("A VitalSignsSpec") {

    describe("given invalid input"){
      it("should throw an error"){

        intercept[Exception] {
          val actualAggregatedResults = testObj.derivePatientResultsAndSaveToFile(invalidHealthRecords, outputDir)
        }
      }
    }

    describe("given no health records"){
      it("should return as expected"){
        val expectedAggregatedResults = aggregatedResultsInFormattedString(0, 0, 0, 0)

        val actualAggregatedResults = testObj.derivePatientResultsAndSaveToFile(emptyFileOfHealthRecords, outputDir)
        //println(actualAggregatedResults)
        verifyPatientResults(List())
        actualAggregatedResults should be (expectedAggregatedResults)
      }
    }

    describe("given valid health records") {
        it("should return as expected") {

          val expectedPatientResults: List[PatientResults] = List(
              PatientResults(1,1,1324,List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT)),
              PatientResults(2,2,1325,List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_BORDERLINE_RESULT, LDL_CLSTRL_BORDERLINE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_BORDERLINE_RESULT, DIABETES_POSITIVE_RESULT)),
              PatientResults(3,3,1349,List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_BORDERLINE_RESULT, LDL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, DIABETES_POSITIVE_RESULT)),
              PatientResults(4,4,1390,List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT)),
              PatientResults(5,5,1432,List(BP_HYPERSTENSIVE_CRISIS_RESULT, TOTAL_CLSTRL_BORDERLINE_RESULT, LDL_CLSTRL_BORDERLINE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_BORDERLINE_RESULT,DIABETES_NORMAL_RESULT)),
              PatientResults(6,6,1460,List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, DIABETES_PRE_RESULT)),
              PatientResults(7,7,1480,List(BP_NORMAL_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_NORMAL_RESULT)),
              PatientResults(8,8,1520,List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_LOW_RESULT)),
              PatientResults(9,9,1549,List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_BORDERLINE_RESULT, LDL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_BORDERLINE_RESULT, DIABETES_NORMAL_RESULT)),
              PatientResults(10,10,1570,List(BP_PREHYPERTENSION_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_VERY_HIGH_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, DIABETES_PRE_RESULT)),
              PatientResults(2,11,1589,List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_DESIRABLE_RESULT, LDL_CLSTRL_DESIRABLE_RESULT, HDL_CLSTRL_LESS_RISK_RESULT, TRI_CLSTRL_DESIRABLE_RESULT, DIABETES_POSITIVE_RESULT)),
              PatientResults(11,12,1621,List(BP_STAGE_1_RESULT, TOTAL_CLSTRL_BORDERLINE_RESULT, LDL_CLSTRL_NEAR_DESIRABLE_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_BORDERLINE_RESULT, DIABETES_NORMAL_RESULT)),
              PatientResults(12,13,1623,List(BP_NORMAL_RESULT, TOTAL_CLSTRL_BORDERLINE_RESULT, LDL_CLSTRL_BORDERLINE_RESULT, HDL_CLSTRL_MAJOR_RISK_RESULT, TRI_CLSTRL_HIGH_RESULT, DIABETES_PRE_RESULT)),
              PatientResults(3,14,1901,List(BP_STAGE_2_RESULT, TOTAL_CLSTRL_HIGH_RESULT, LDL_CLSTRL_HIGH_RESULT, HDL_CLSTRL_NORMAL_RESULT, TRI_CLSTRL_VERY_HIGH_RESULT, DIABETES_PRE_RESULT))
          )

          // 12 distinct patients
          // 8 patients with high blood pressure (most recent result for a patient is considered) (healthRecord Ids of 4,5,6,8,9,12,11,& 14)
          // 2 patients with high cholesterol (patientIds of 6 & 10)
          // 1 patient with diabetes (patientId of 2)(a patient must have 2 positive results for diabetes to officially be diagnosed with diabetes)
          // 2 healthy patients (patientIds of 1 & 7)


          val expectedAggregatedResults = aggregatedResultsInFormattedString(67, 17, 8, 17)

          val actualAggregatedResults = testObj.derivePatientResultsAndSaveToFile(validHealthRecords, outputDir)

          verifyPatientResults(expectedPatientResults)
          actualAggregatedResults should be (expectedAggregatedResults)
        }
      }

    }

    def verifyPatientResults(expectedPatientResults:List[PatientResults]) = {
      val patientResults = sc.textFile(outputDir).map(
        line => {
          implicit val formats = DefaultFormats
          parse(line.toString).extract[PatientResults]
        })
      patientResults.sortBy(_.timestamp).collect should be (expectedPatientResults)
    }

    def aggregatedResultsInFormattedString(percentageWithHighBloodPressure:Int,
                                           percentageWithHighCholesterol:Int,
                                           percentageWithDiabetes:Int,
                                           percentageOfHealthyPatients:Int) = {
      "******************************************\n" +
        s"Patients with High Blood Pressure: $percentageWithHighBloodPressure%\n" +
        s"Patients with High Cholesterol: $percentageWithHighCholesterol%\n" +
        s"Patients with Diabetes: $percentageWithDiabetes%\n" +
        "******************************************\n" +
        s"Healthy Patients: $percentageOfHealthyPatients%\n" +
        "******************************************\n"
    }
}
