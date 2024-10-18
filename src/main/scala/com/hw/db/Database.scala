package com.hw.db

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, explode, size}

import scala.util.{Failure, Success, Try}

object Database {
  def upload(session: SparkSession, jsonFilePath: String): Unit = {
    val jsonDataFrame = session.read.json(jsonFilePath)

    jsonDataFrame
      .write
      .format("mongodb")
      .mode("append")
      .save()
  }

  def searchForWords(session: SparkSession, words: String): Try[Array[String]] = {
    val wordsDataFrame = session
      .read
      .format("mongodb")
      .load

    wordsDataFrame.show(false)

    val queryWords = words.toLowerCase.split(" ")

    val filteredDataFrame = wordsDataFrame
      .filter(col("word").isin(queryWords: _*))
      .select(col("word"), explode(col("document_list")).as("doc"))

    val resultDocs = filterBasedOnWords(filteredDataFrame, queryWords)

    if (resultDocs.isEmpty) {
      return Failure(new Exception("No docs contain all queried words"))
    }

    Success(resultDocs)
  }

  private def filterBasedOnWords(filteredDataFrame: DataFrame, queryWords: Array[String]) = filteredDataFrame
      .groupBy("doc")
      .agg(collect_set("word").as("words"))
      .filter(size(col("words")) === queryWords)
      .select("doc")
      .collect()
      .map(_.toString())
}
