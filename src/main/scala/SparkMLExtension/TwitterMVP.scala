package SparkMLExtension

import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.{RegexTokenizer, HashingTF, IDF}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import scala.util.parsing.json._

//This creates a basic ML model using twitter data

object TwitterMVP {

  def main(args: Array[String]) {

    val sc = SparkMLExtension.CreateContext.main(args)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val tweets = sc.textFile("file:///Users/conor/dsci6009/Tweets/conor-twitterdata-1-2017-04-12-18-01-25-35b3cf72-d1d3-4462-a67a-dde73bea8c74")
      .map(getTweetsAndLang)
      .filter(x => x._2 != -1)
      .toDF()

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("_1")
      .setOutputCol("words")
      .setPattern("\\W")
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20) // TODO: change num features if you scale
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val rf = new RandomForestClassifier()
      .setLabelCol("_2")
      .setFeaturesCol("features")
      .setNumTrees(10) // TODO: change num trees if you scale

    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer, hashingTF, idf, rf))

    val model = pipeline.fit(tweets)

  }

  def findVal(str: String, ToFind: String): String = {
    JSON.parseFull(str) match {
      case Some(m: Map[String, String]) => m(ToFind)
    }
  }


  def getTweetsAndLang(input: String): (String, Int) = {
    try {
      var result = (findVal(input, "text"), -1)

      if (findVal(input, "lang") == "en") result.copy(_2 = 0)
      else if (findVal(input, "lang") == "es") result.copy(_2 = 1)
      else result
    } catch {
      case e: Exception => ("unknown", -1)
    }
  }
}