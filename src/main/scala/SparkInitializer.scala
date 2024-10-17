import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object SparkInitializer {
  def init(): SparkContext = {
    initSession().sparkContext
  }

  def initSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Simple Search Engine")
      .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/bigdata.words")
      .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/bigdata.words")
      .getOrCreate()
  }
}