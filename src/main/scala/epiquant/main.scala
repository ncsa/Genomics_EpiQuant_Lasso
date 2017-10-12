package epiquant
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
//import org.apache.spark.mllib.regression.{LassoModel, LassoWithSGD}

object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = org.apache.spark.sql.SparkSession.builder
      .appName("LASSO regression")
      .getOrCreate()

    //val phe_length = DataWriter.write(snp_f, phe_f, out_f)
    val out_f = args(0)
    val phe_length = args(1).toInt

    val data: RDD[(DenseVector[Double], DenseMatrix[Double])] = Parser.parse(spark, out_f, phe_length)
    // Cache to memory
    data.cache()
    data.count  // To trigger the caching

    // Setting up model
    var t1 = System.currentTimeMillis()
    val model = new ADMM(data.map(_._2), data.map(_._1), spark.sparkContext)
    model.set_opts(100, 1e-3, 1e-3)
    model.set_lambda(0.01)
    var t2 = System.currentTimeMillis()

    // Generate grids of lambdas
    //val lambda = lambdas(n, 0.001, 0.1)
    //val neglll = new DenseVector[Double](lambda.length)
    //val betas = new Array[(Double, SparseVector[Double])](lambda.length)

    t1 = System.currentTimeMillis()
    model.run()
    t2 = System.currentTimeMillis()
    println("Iteration time: " + (t2 - t1) / 1000.0 + "s")
    print ("Estimated Coefficients: " + model.coef)

    data.unpersist()

  }

}
