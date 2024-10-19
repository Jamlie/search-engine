import com.hw.db.Database
import lib.fmt.fmt
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender

import scala.util.{Failure, Success}

object Client {
  def main(args: Array[String]): Unit = {
    BasicConfigurator.configure(new NullAppender())

    print("Enter something to search for: ")
    val words = scala.io.StdIn.readLine()

    val output = Database.searchForWords(SparkInitializer.initSession, words)

    output match {
      case Failure(exception) => {
        fmt.eprintln(exception.getMessage)
        sys.exit
      }
      case Success(value) => {
        fmt.println(s"Word is found in docs: ${value.mkString("Array(", ", ", ")")}")
      }
    }
  }
}
