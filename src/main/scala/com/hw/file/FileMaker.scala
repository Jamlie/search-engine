package com.hw.file

import org.apache.spark.SparkContext

import java.io.File
import java.nio.file.Paths

class FileMaker(val sc: SparkContext) {

  def make(sentencesDir: String, outputFileName: String): Unit = {
    val sep = File.separator
    val allFiles = s"${sentencesDir}${sep}*"

    val txtFilesRDD = sc.wholeTextFiles(allFiles)

    val wordDocPairs = txtFilesRDD.flatMap({
      case (filePath, content) =>
        val words = content.split("\\s+")
          .map(_.replaceAll(",", ""))
          .filter(_.nonEmpty).map(_.toLowerCase)
        words.distinct.map(word => (word, filePath.split("/").last))
    })

    val wordToDocList = wordDocPairs
      .groupByKey()
      .mapValues(docList => docList.toSet.toList.sorted)

    val invertedIndex = wordToDocList.map({
      case (word, doc) => (word, doc.size, doc.mkString(", "))
    })

    val sortedInvertedIndex = invertedIndex.sortBy(_._1)

    val formattedOutput = sortedInvertedIndex.map {
      case (word, count, docs) => s"$word, $count, $docs"
    }

    formattedOutput.coalesce(1, shuffle = true)
      .saveAsTextFile(s"${Paths.get(".").toString}${sep}$outputFileName")
  }
}
