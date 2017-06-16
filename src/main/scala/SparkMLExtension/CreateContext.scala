package SparkMLExtension

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object CreateContext {

  def main(args: Array[String]): SparkContext = {

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    sc
  }
}
