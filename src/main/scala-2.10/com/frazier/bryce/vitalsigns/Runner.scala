package com.frazier.bryce.vitalsigns

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
/**
  * Created by bryce frazier on 4/10/16.
  */

import org.apache.spark.{SparkConf, SparkContext}

object Runner {
  def main(args: Array[String]) {
    val healthRecordInputFile = args(0)
    val patientRecordOutputFile = args(1)

    val outputPath = new Path(patientRecordOutputFile)
    val fs = FileSystem.get(new Configuration)
    if(fs.exists(outputPath)) fs.delete(outputPath, true)

    val aggregatedResults = VitalSigns().derivePatientResultsAndSaveToFile(
                                                            healthRecordInputFile,
                                                                patientRecordOutputFile)

    println(aggregatedResults)
  }
}

object Spark {
  val sparkConf = new SparkConf().setAppName("VitalSigns").setMaster("local[1]")
  val ctx = new SparkContext(sparkConf)
}


// bloodPressure is represented as "118/74"
// in units: mm Hg, where the numerator is Systolic, and the denominator is Diastolic
// and read as "118 over 74 millimeters of mercury"


// cholesterol figures are in mg/dL (U.S. & other countries). Canada & most of Europe use mmol/L

//  glucoseLvl
//  # < 45 => Abnormally low
//  from  45 to 99 mg/DL (~3 to 5.5 mmol/L)  	Normal fasting glucose  //http://www.healthieryou.com/hypo.html
//    from 100 to 125 mg/DL(5.6 to 6.9 mmol/L)   	Prediabetes (impaired fasting glucose)
//  from 126 mg/DL(7.0 mmol/L) 				  Diabetes   (if only one => declare need to take another to be more decisive)
//  and above on more than one testing occasion			(logic counting if any labeled Diabetes)
//

case class Cholesterol(total: Int, ldl: Int, hdl: Int, triglycerides: Int)

case class HealthRecord(id: Long, patientId: Long, age: Long, gender: String, region: String, timestamp: Long, glucoseLvl: Long, cholesterol: Cholesterol, heartRate: Long, bloodPressure: String)

case class HealthResult(name: String, status: String, interpretation: String)

case class PatientResults(patientId: Long, recordId: Long, timestamp: Long, healthResults: List[HealthResult])

