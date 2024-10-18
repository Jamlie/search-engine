package com.hw.convertor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TokenParser {
  def json(session: SparkSession, path: String, jsonFilePath: String): Unit = {
    val dataFrame = session.read.text(path)

    val structuredDataFrame = parseData(dataFrame)

    structuredDataFrame.show(false)

    saveAsJson(structuredDataFrame, jsonFilePath)
  }

  private def parseData(dataFrame: DataFrame) = dataFrame
    .withColumn("split_data", split(col("value"), ",\\s*"))
    .select(
      col("split_data").getItem(0).as("word"),
      col("split_data").getItem(1).as("count"),
      expr("slice(split_data, 3, size(split_data))").as("document_list")
    )

  private def saveAsJson(structuredDataFrame: DataFrame, jsonFilePath: String): Unit = structuredDataFrame
    .write
    .json(jsonFilePath)
}
