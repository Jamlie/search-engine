package com.hw.query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query {
  def build(session: SparkSession): Unit = {
    val wordsDataFrame = session
      .read
      .format("mongodb")
      .load

    wordsDataFrame.show(false)

    val userQuery = "all fucking"
    val queryWords = userQuery.toLowerCase.split(" ")

    val filteredDataFrame = wordsDataFrame
      .filter(col("word").isin(queryWords: _*))
      .select(col("word"), explode(col("document_list")).as("doc"))

    val docsWithWordCount = filteredDataFrame
      .groupBy("doc")
      .agg(collect_set("word").as("words"))
      .filter(size(col("words")) === queryWords.length)

    val resultDocs = docsWithWordCount
      .select("doc")
      .collect
      .map(_.toString)

    if (resultDocs.isEmpty) {
      println("no docs contain all queried words")
    } else {
      println(s"docs containing all queried words: ${resultDocs.mkString(", ")}")
    }
  }
}
