package SparkMLExtension

import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import SparkMLExtension.TweetParser
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer


trait SimpleIndexerParams extends Params {
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")

//  setDefault(inputCol, Array[String]())

//  final def getIsInList: Array[String] = $(isInList)
}

class SimpleIndexer(override val uid: String)
  extends Estimator[SimpleIndexerModel] with SimpleIndexerParams {

  def setInputCol(value: String) = set(inputCol, value)

  def setOutputCol(value: String) = set(outputCol, value)

  def this() = this(Identifiable.randomUID("simpleindexer"))

  override def copy(extra: ParamMap): SimpleIndexer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def fit(dataset: Dataset[_]): SimpleIndexerModel = {
    import dataset.sparkSession.implicits._
    val words = dataset.select(dataset($(inputCol)).as[String]).distinct
      .collect()
    new SimpleIndexerModel(uid, words)
  }
}

class SimpleIndexerModel(override val uid: String, words: Array[String])
  extends Model[SimpleIndexerModel] with SimpleIndexerParams {

  override def copy(extra: ParamMap): SimpleIndexerModel = {
    defaultCopy(extra)
  }

  private val labelToIndex: Map[String, Double] = words.zipWithIndex.
    map{case (x, y) => (x, y.toDouble)}.toMap

  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val indexer = udf { label: String => labelToIndex(label) }
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as($(outputCol)))
  }
}
//end::SimpleIndexer[]

object Runner {
  def main(args: Array[String]) {
    val sc = SparkMLExtension.CreateContext.main(args)
    val sqlContext = new SQLContext(sc)

    val tweets = sqlContext.read.textFile("file:///Users/conor/dsci6009/Tweets/conor-twitterdata-1-2017-04-12-18-01-25-35b3cf72-d1d3-4462-a67a-dde73bea8c74")
//    val Array(trainingData, testData) = tweets.randomSplit(Array(0.7, 0.3))

//    val transformer = new TweetParser()
//    val indexer = new StringIndexer()
//      .setInputCol("lang")
//      .setOutputCol("index")



    val df = sqlContext.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "inputCol")

    val indexer = new SimpleIndexer()
      .setInputCol("inputCol")
      .setOutputCol("categoryIndex")

    val indexed = indexer.fit(df).transform(df.select("inputCol")) // TODO: this fails
//    indexed.show()



//    val pipeline = new Pipeline()
//      .setStages(Array(transformer, indexer))

//    pipeline.fit(tweets).transform(tweets).show()

  }
}