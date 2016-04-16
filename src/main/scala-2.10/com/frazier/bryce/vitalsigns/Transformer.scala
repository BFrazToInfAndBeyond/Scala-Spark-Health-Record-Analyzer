package com.frazier.bryce.vitalsigns

import org.apache.spark.rdd.RDD

/**
  * Created by bryce frazier on 4/10/16.
  */
class Transformer {

  def groupByRegion(healthRecords: RDD[HealthRecord]): RDD[(String, Iterable[HealthRecord])] = {
    healthRecords.map(record => (record.region, record)).groupByKey()
  }

  def groupHealthResultsByRecordId(healthResults: RDD[(Long, HealthResult)]) = {
    healthResults.groupByKey().map { case (k, v) => k -> v.toList }
  }

  def obtainPatientResults(healthResults_groupedByRecordId: RDD[(Long, List[HealthResult])],
                           healthRecords: RDD[HealthRecord]): RDD[PatientResults] = {
    val extractedMinimalRecordInfo_keyByRecordId = healthRecords.map {
      case record => (record.id, (record.patientId, record.timestamp))
    }

    val patientResults: RDD[PatientResults] =
      healthResults_groupedByRecordId.join(extractedMinimalRecordInfo_keyByRecordId).map {
        case (recordId, (healthResults, (patientId, timestamp))) =>
          PatientResults(patientId, recordId, timestamp, healthResults)
      }
    patientResults
  }

  def groupByPatientId_sortedByTimestamp(healthRecords: RDD[HealthRecord]): RDD[(Long, List[HealthRecord])] = {
    healthRecords.map(record => (record.patientId, record)).groupByKey().map{ case (k, v) => (k, v.toList.sortBy(_.timestamp))}
  }

  def groupByPatientId_latestRecord(healthRecords: RDD[HealthRecord]): RDD[(Long, HealthRecord)] = {
    groupByPatientId_sortedByTimestamp(healthRecords).map { case (k, v) => (k, v.last) }
  }
}

object Transformer {
  def apply() = new Transformer()
}

