package SparkMLExtension

//import org.apache.spark.sql.{DataFrame, Row, SQLContext}
//import org.apache.spark.rdd
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import scala.util.parsing.json._

object TwitterMVP {

  def main(args: Array[String]) {

    val sc = SparkMLExtension.CreateContext.main(args)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val tweets = sc.textFile("file:///Users/conor/dsci6009/Tweets/conor-twitterdata-1-2017-04-12-18-01-25-35b3cf72-d1d3-4462-a67a-dde73bea8c74")
      .map(getTweetsAndLang)
      .filter(x => x(1) != "unknown")
      .take(3).foreach(println)
//      .toDF() //.printSchema

    val schemaString = "name age"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

//    val l = List(("tweet", StringType), ("lang", IntegerType))

//        .map(_ => StructField(_._1, _._2, nullable = true))

    val schema = Seq(StructField("tweet", StringType, nullable = true), StructField("lang", IntegerType, nullable = true))
//    sqlContext.createDataFrame(tweets, StructType(schema)).take(3)

//    val schema = StructType(StructField("tweet", StringType, nullable = true) ::: StructField("lang", IntegerType, nullable = true))

//    println(StructField("example", IntegerType, nullable = true))

//    val fields = List[StructField](("tweet", StringType), ("lang", IntegerType))
//    val schema = org.apache.spark.sql.types.StructType(StructField("tweet", StringType, nullable = true), StructField("lang", IntegerType, nullable = true))

//    println(schema)

//    tweets.take(10).foreach(println)
  }

  def findVal(str: String, ToFind: String): String = {
    JSON.parseFull(str) match {
      case Some(m: Map[String, String]) => m(ToFind)
    }
  }

  def getTweetsAndLang(input: String): List[String] = {
    try {
      var result = List[String](findVal(input, "text"), "unknown")

      if (findVal(input, "lang") == "en") result.updated(1, "0")
      else if (findVal(input, "lang") == "es") result.updated(1, "1")
      else result
    } catch {
      case e: Exception => List[String]("unknown", "unknown")
    }
  }
}