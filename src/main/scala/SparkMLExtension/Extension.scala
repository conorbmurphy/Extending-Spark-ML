package SparkMLExtension

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.util.Identifiable
import SparkMLExtension.TwitterMVP.findVal


class TweetParser(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("hardcodedwordcount"))

  def copy(extra: ParamMap): TweetParser = {
    defaultCopy(extra)
  }
  //end::basicPipelineSetup[]

  //tag::basicTransformSchema[]
  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex("value")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField("tweet", StringType, true))
        .add(StructField("lang", StringType, true))
  }
  //end::basicTransformSchema[]

  //tag::transformFunction[]
  def transform(df: Dataset[_]): DataFrame = {
    val tweet = udf { in: String => findVal(in, "text") }
    val lang = udf { in: String => findVal(in, "lang")}
    df.select(col("*"),
        tweet(df.col("value")).as("tweet"),
        lang(df.col("value")).as("lang"))
      .filter("tweet is not null")
  }
  //end::transformFunction[]
}

object Extension {
  def main(args: Array[String]) {
      val sc = SparkMLExtension.CreateContext.main(args)
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._ // gives me toDF()
      val tweets = sc.textFile("file:///Users/conor/dsci6009/Tweets/conor-twitterdata-1-2017-04-12-18-01-25-35b3cf72-d1d3-4462-a67a-dde73bea8c74")

      val transformer = new TweetParser()
      tweets.toDF().show()
      transformer.transform(tweets.toDF()).show()
  }
}
