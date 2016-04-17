package com.frazier.bryce.vitalsigns

import net.liftweb.json._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by bryce frazier on 4/16/16.
  */
class VitalSigns {

  def getSparkContext: SparkContext = Spark.ctx

  def derivePatientResultsAndSaveToFile(healthRecordInputFile: String,
                 patientRecordOutputFile: String) = {

    // read healthRecords from input file
    val healthRecords = getSparkContext.textFile(healthRecordInputFile).map(
      line => {
        implicit val formats = DefaultFormats
        parse(line.toString).extract[HealthRecord]
    })

    val patientResults:RDD[PatientResults] = VitalSigns().obtainPatientResults(healthRecords)

    // save patientResults to output text file
    patientResults.map(result => {
      implicit val formats = DefaultFormats
      compact(render(Extraction.decompose(result)))
    }).saveAsTextFile(patientRecordOutputFile)

    Aggregator().obtainAggregatedResults(patientResults)
  }

  private def obtainPatientResults(healthRecords:RDD[HealthRecord]) = {
    val bloodPressureResults = BloodPressureProcessor().obtainBloodPressureResults(healthRecords)
    val cholesterolResults = CholesterolProcessor().obtainCholesterolResults(healthRecords)
    val diabetesResults = DiabetesProcessor().obtainDiabetesResults(healthRecords)

    val healthResults = bloodPressureResults.union(cholesterolResults).union(diabetesResults)
    val transformer = Transformer()
    val resultsByRecordId = transformer.groupHealthResultsByRecordId(healthResults)

    transformer.obtainPatientResults(resultsByRecordId, healthRecords)
  }
}

object VitalSigns {
  def apply() = new VitalSigns()
}