import com.hw.convertor.TokenParser
import com.hw.db.Database
import com.hw.file.FileMaker
import com.hw.query.Query
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.SparkContext

import java.nio.file.Paths
import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure(new NullAppender())

    val sc = SparkInitializer.init()
    val session = SparkInitializer.initSession()

    val outputDir = unwrap(Try(Paths.get(".", "output").toString))
    val outputFile = unwrap(Try(Paths.get(outputDir, "part-00000").toString))
    val jsonFileOutput = unwrap(Try(Paths.get(".", "invertedIndexJSON").toString))

    createTxtFile(sc, outputDir)
    txtToJson(outputFile, jsonFileOutput)
    Database.upload(session, jsonFileOutput)
    Query.build(session)

    session.stop()
    sc.stop()
  }

  private def createTxtFile(sc: SparkContext, outputFile: String): Unit = {
      val fileMaker = new FileMaker(sc)
      val sentencesDir = unwrap(Try(Paths.get(".", "sentences").toString))
      fileMaker.make(sentencesDir, outputFile)
  }

  private def txtToJson(outputFile: String, jsonFileOutput: String): Unit = {
    TokenParser.json(SparkInitializer.initSession(), outputFile, jsonFileOutput)
  }

  private def unwrap[T](optional: Try[T]): T = {
    optional match {
      case Failure(exception) => {
        println(exception)
        sys.exit(0)
      }
      case Success(value) => value
    }
  }
}
