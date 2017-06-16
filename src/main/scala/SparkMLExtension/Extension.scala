package SparkMLExtension

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StringType, IntegerType}
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.ml.util.Identifiable


class HardCodedWordCountStage(override val uid: String) extends Transformer {
  def this() = this(Identifiable.randomUID("hardcodedwordcount"))

  def copy(extra: ParamMap): HardCodedWordCountStage = {
    defaultCopy(extra)
  }
  //end::basicPipelineSetup[]

  //tag::basicTransformSchema[]
  override def transformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex("happy_pandas")
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(
        s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField("happy_panda_counts", IntegerType, false))
  }
  //end::basicTransformSchema[]

  //tag::transformFunction[]
  def transform(df: Dataset[_]): DataFrame = {
    val wordcount = udf { in: String => in.split(" ").size }
    df.select(col("*"),
      wordcount(df.col("happy_pandas")).as("happy_panda_counts"))
  }
  //end::transformFunction[]
}

object Extension {
  def main(args: Array[String]) {
      val sc = SparkMLExtension.CreateContext.main(args)

      val adding = sc.parallelize(1 to 100).sum
      println("Success!  Returned: " + adding)
  }
}
