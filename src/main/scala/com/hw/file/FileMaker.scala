package com.hw.file

import org.apache.spark.SparkContext

import java.io.File
import java.nio.file.Paths

class FileMaker(val sc: SparkContext) {

  def make(sentencesDir: String, outputFileName: String): Unit = {
    val sep = File.separator
    val allFiles = s"${sentencesDir}${sep}*"

    sc.wholeTextFiles(allFiles)
      .flatMap {
        case (filePath, content) =>
          content.split("\\s+")
            .map(_.replaceAll(",", ""))
            .filter(_.nonEmpty)
            .map(_.toLowerCase)
            .distinct
            .map(word => (word, filePath.split("/").last))
      }
      .groupByKey
      .mapValues(docList => docList.toSet.toList.sorted)
      .map {
        case (word, doc) => (word, doc.size, doc.mkString(", "))
      }
      .sortBy(_._1)
      .map {
        case (word, count, docs) => s"$word, $count, $docs"
      }
      .coalesce(1, shuffle = true)
      .saveAsTextFile(s"${Paths.get(".").toString}${sep}$outputFileName")
  }
}
