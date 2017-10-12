package epiquant


import org.junit.Test
import org.junit.Before
import org.junit.Assert
import epiquant.Parser
import org.apache.log4j.{Level, Logger};

class TestParser {

  @Before
  def testCreateParser(): Unit ={

  }

  @Test
  def testParse(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("LASSO regression")
      .getOrCreate()

    val args = "epiquant -s <snp_file> -p <phe-file>"
    /*val (dataset, snp_names, phe_names, samples) = Parser.parse(spark, args)
    val num_part = dataset.getNumPartitions
    val m = samples.length / num_part
    val n = snp_names.length
    */

  }


}
