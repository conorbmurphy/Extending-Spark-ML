package SparkMLExtension

import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.Pipeline


trait SimpleIndexerParams extends Params {
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")

  /*
   * Note the next two lines.  It's currently unclear to my why these lines are
   * needed.  Without them, `fit` works just find.  `transform`, however,
   * does not as it fails to find these columns.  This is a hard-coded version
   * See our issue here for details:
   * https://github.com/high-performance-spark/high-performance-spark-examples/issues/89
   * UPDATE: we solved this with PR:
   * https://github.com/high-performance-spark/high-performance-spark-examples/pull/90
  */

//  setDefault(inputCol, "lang") // Hard codes defaults (no longer needed)
//  setDefault(outputCol, "categoryIndex") // Hard codes defaults (no longer needed)

//  final def getInputCol: String = $(inputCol)
//  final def getOutputCol: String = $(outputCol)
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
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def fit(dataset: Dataset[_]): SimpleIndexerModel = {
    import dataset.sparkSession.implicits._
    val words = dataset.select(dataset($(inputCol)).as[String]).distinct
      .collect()
    val model = new SimpleIndexerModel(uid, words)
    model.set(inputCol, $(inputCol))
    model.set(outputCol, $(outputCol))
    model
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
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val indexer = udf { label: String => labelToIndex(label) }
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as($(outputCol)))
  }
}


object Runner {
  def main(args: Array[String]) {
    val sc = SparkMLExtension.CreateContext.main(args)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)

    val tweets = sqlContext.read.textFile("file:///Users/conor/dsci6009/Tweets/conor-twitterdata-1-2017-04-12-18-01-25-35b3cf72-d1d3-4462-a67a-dde73bea8c74")
    val transformer = new TweetParser()
    val indexer = new SimpleIndexer()
      .setInputCol("lang")
      .setOutputCol("categoryIndex")

    val pipeline = new Pipeline()
      .setStages(Array(transformer, indexer))
    pipeline.fit(tweets).transform(tweets).show()

  }
}