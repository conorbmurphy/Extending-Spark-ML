package SparkMLExtension

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.util.Identifiable
import SparkMLExtension.TwitterMVP.findVal


class TweetParser(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("TweetParser"))

  def copy(extra: ParamMap): TweetParser = { // required of of Transformer
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {

    val idx = schema.fieldIndex("value")
    val field = schema.fields(idx)
    if (field.dataType != StringType) { // Check that the input type is a string
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }

    schema.add(StructField("tweet", StringType, true)) // Add the return field - 2 cols
        .add(StructField("lang", StringType, true))
  }

  def transform(df: Dataset[_]): DataFrame = { // Where the magic happens!
    val tweet = udf { in: String => findVal(in, "text") }
    val lang = udf { in: String => findVal(in, "lang")}
    df.select(col("*"),
        tweet(df.col("value")).as("tweet"),
        lang(df.col("value")).as("lang"))
      .filter("tweet is not null")
  }
}

object Extension {
  def main(args: Array[String]) {
      val sc = SparkMLExtension.CreateContext.main(args)
      val sqlContext = new SQLContext(sc)

      val tweets = sqlContext.read.textFile("file:///Users/conor/dsci6009/Tweets/conor-twitterdata-1-2017-04-12-18-01-25-35b3cf72-d1d3-4462-a67a-dde73bea8c74")

      val transformer = new TweetParser()
      transformer.transform(tweets).show()
  }
}
