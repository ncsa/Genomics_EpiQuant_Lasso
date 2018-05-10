package epiquant
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
//import org.apache.spark.mllib.regression.{LassoModel, LassoWithSGD}

object Main {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("LASSO")
    val sc = new SparkContext(conf)
    val dir = getClass.getResource("").getPath
    println(dir)
    sc.setCheckpointDir(dir)

    /*if(sc.master.equals("local")){
      sc.setCheckpointDir("/Users/dlaw/foo")
    }else{
      sc.setCheckpointDir(s"hdfs://${sc.master.substring(8,sc.master.length-5)}:9000/root/scratch")
    }*/

    //val phe_length = DataWriter.write(snp_f, phe_f, out_f)
    require(args(0) != null, "Missing Input Genotype File!")
    val genofile = args(0)
    require(args(1) != null, "Missing Input Phenotype File!")
    val phenofile = args(1)
    val iter = if (args.length < 3) 1 else args(2).toInt
    val abstol = if (args.length < 4) 1e-7 else args(3).toDouble
    val reltol = if (args.length < 5) 1e-7 else args(4).toDouble
    val lambda = if (args.length < 6) 1 else args(5).toDouble


    //val genofile = "resources/Genotypes/10.Subset.n=2648.tsv"
    //val phenofile = "resources/Phenotypes/Simulated.Data.1.Reps.Herit.0.92_n=2648.tsv"

    val (genoData, phenoData) = Parser.parse2(sc, genofile, phenofile)
    val admm = new ADMM(genoData, phenoData, sc)
    admm.set_opts(max_iter = iter, eps_abs = abstol, eps_rel = abstol, logs = true, lambda = lambda)
    var t1 = System.currentTimeMillis()
    admm.run()
    var t2 = System.currentTimeMillis()
    println("The Coefficients: ")
    println(admm.coef)
    println("the x values: ")
    println(admm.xbar)
    println("the u values: ")
    println(admm.ubar)
    println("the z values: ")
    println(admm.zbar.toDenseVector)


    /*val lambdas = Array(0.03)//, 0.001, 0.01, 0.05, 0.1, 1, 2, 3)
    val (min_lambda_idx, mse, best_model) = cross_validate(admm, lambdas, admm.snp_names)
    println("Best lambda index: " + min_lambda_idx)
    println("Best lambda: " + lambdas(min_lambda_idx))
    println("MSE: " + mse)
    println ("Estimated Coefficients: ")
    //best_model.coefficients.toArray.zip(names).map(x => if (x._1 > 0.001) println(x._2 + ": " + x._1))
    best_model.coefficients.toArray.foreach(println)*/

    //foreach(println)
    //data.unpersist();


  }


  /*def cross_validate(model: ADMM, lambdas: Array[Double], names: Array[String]): (Int, Double, LassoModel)={
    var t1 = System.currentTimeMillis()
    var t2 = System.currentTimeMillis()

    val mses = Array.fill(lambdas.length)(0.0)
    var coefs = new ListBuffer[SparseVector[Double]]()
    var i = 0;
    for (lambda <- lambdas) {
      model.set_lambda(lambda)
      t1 = System.currentTimeMillis()
      model.run()
      t2 = System.currentTimeMillis()
      println("Iteration time: " + (t2 - t1) / 1000.0 + "s")
      println("Iterations: " + model.niter)
      //
      println("Mean Square error: " + model.mse)
      mses(i) = model.mse
      i += 1
    }
    val coef1 = model.coef//.foldLeft(DenseVector[Double]())((x, y) => DenseVector.vertcat(x,y))
    print(mses.indexOf(mses.min))
    val best_model = new LassoModel(coef1, 0.0, names.toVector) //coefs.toList(mses.indexOf(mses.min))
    return (mses.indexOf(mses.min), mses.min, best_model)
  }*/


}
