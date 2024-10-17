package com.hw.db

import org.apache.spark.sql.SparkSession

object Database {
  def upload(session: SparkSession, jsonFilePath: String): Unit = {
    val jsonDataFrame = session.read.json(jsonFilePath)

    jsonDataFrame
      .write
      .format("mongodb")
      .mode("append")
      .save()
  }
}
