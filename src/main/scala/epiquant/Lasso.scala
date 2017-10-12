package epiquant

import scala.util.control.Breaks
import org.apache.log4j.Logger
import org.apache.log4j.Level
import epiquant.Parser

import scala.util.control.Breaks._
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector}
import breeze.linalg._
import breeze.optimize.proximal.QuadraticMinimizer
import breeze.numerics._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession._
import org.apache.spark.{SparkContext, storage}


//import org.apache.spark.mllib.linalg.{DenseVector, DenseMatrix, Vector, Vectors}

class Lasso {

}

object ADMM1 extends Lasso {

  /* dataset: RDD of y and X
  * n: number of samples
  * d: dimensionality, or number of SNPs
  * lambda: regParam
  * tol: convergence tolerence level
  */

  /*val maxIter = 100
  val lambda = 0.5
  val absTol = 1e-4
  val relTol = 1e-2
  var rho = 1.0
  val n = snp_names.length
  val D = n*(n+1)/2*/
  @Deprecated
  def fit(dataset: RDD[(DenseMatrix[Double], DenseMatrix[Double], Int)], Phe: Int, D: Int, maxIter: Int, lambda: Double, rho: Double, absTol: Double, relTol: Double) {
    val num_part = dataset.getNumPartitions
    var cholesky_A = dataset.mapPartitions {
      iterator => {
        val g: (DenseMatrix[Double], DenseMatrix[Double], Int) = iterator.next
        val snp_matrix = g._1
        val phe_matrix = g._2
        val num_rows = g._3 //number of samples in this partition
        //precompute Atb

        val skinny = num_rows >= D
        //TODO compare with breeze matrix vector multiplication speed

        val Atb = snp_matrix.t * phe_matrix(::, Phe) //.map(dv => (snp_matrix.t *dv))

        var L = new DenseMatrix(1, 1, Array(0.0))
        if (skinny) {
          //Compute the matrix A: A = chol(AtA + rho*I)
          val AtA = snp_matrix.t * snp_matrix
          //new DenseMatrix(D, D, Array.fill[Double](D*D)(0))
          val rhoI = DenseMatrix.eye[Double](D)
          rhoI *= rho
          L = cholesky(AtA + rhoI) /* AtA+rhoI = L*L**T.*/
        } else {
          //compute the matrix L : L = chol(I + 1/rho*AAt)
          val AAt = snp_matrix * snp_matrix.t
          AAt *= 1 / rho
          val eye = DenseMatrix.eye[Double](num_rows)
          L = cholesky(AAt + eye) /* AAt+I = L*L**T.*/
        }

        val x = DenseVector.zeros[Double](D)
        val u = DenseVector.zeros[Double](D)
        val zprev = DenseVector.zeros[Double](D)

        //TODO find out a way to map Atb
        Iterator((L, Atb, num_rows, x, u, zprev, snp_matrix))
      }
    }.persist(storage.StorageLevel.MEMORY_AND_DISK)

    val zprev = DenseVector.zeros[Double](D)
    //val zdiff = DenseVector.zeros[Double](D)
    val z = DenseVector.zeros[Double](D)
    var iter = 0
    var prires = 0.0
    var nxstack = 0.0
    var nystack = 0.0
    var bc_z = cholesky_A.sparkContext.broadcast(z)

    val startTime = System.nanoTime()


    breakable {
      while (iter < maxIter) {
        cholesky_A = cholesky_A.mapPartitions {
          iterator => {
            //          var i =0
            //println(i)
            //var g = iterator.next
            //while(iterator.hasNext){
            //  i+=1
            //  println(i) //make sure only 1 matrix per partition
            // }
            val G = iterator.next()

            val L = G._1
            val Atb = G._2
            val num_rows = G._3
            val x = G._4
            val z = bc_z.value
            val u = G._5
            val zprev = z.copy
            val A = G._7 //snp_matrix
            val skinny = num_rows >= D

            // u-update: u = u + x - z
            u :+= x
            u :-= z
            // x-update: x = (A^T A + rho I) \ (A^T b + rho z - y)
            val q = z.copy
            q :-= u
            q :*= rho
            q :+= Atb

            if (skinny) {
              solve(L, q, x)
            } else {
              //val Aq = DenseVector.zeros[Double](n) // new DenseVector(Array.fill[Double](N)(0))
              //QuadraticMinimizer.gemv(1, L, q, 0, Aq)
              val p = DenseVector.zeros[Double](num_rows)
              val Aq = A * q
              solve(L, Aq, p)
              //BLAS.gemv(1, fromBreeze(A), fromBreeze(q), 0, Aq)
              //solve(A, asBreeze(Aq), p)
              //gsl_blas_dgemv(CblasTrans, 1, A, b, 0, Atb); // Atb = A^T b
              QuadraticMinimizer.gemv(1, A.t, p, 0, x)
              x :*= -1 / (rho * rho)
              q :*= 1 / rho
              x :+= q
            }


            Iterator((L, Atb, num_rows, x, u, zprev, A))
          }

        } //end MapPartition
        /*
          * Message-passing: compute the global sum over all processors of the
          * contents of w and t. Also, update z.
          */

        val recv = cholesky_A.map {
          A => {
            val x = A._4
            val z = bc_z.value
            val u = A._5

            val r = x - z
            val r_sq = r.data.map(x => x * x).sum
            val x_sq = x.data.map(x => x * x).sum
            val u_sq = u.data.map(x => x * x).sum / (rho * rho)

            Array(r_sq, x_sq, u_sq)
          }
        }.treeReduce((x, y) => Array(x(0) + y(0), x(1) + y(1), x(2) + y(2)), 2)

        val new_z = cholesky_A.map {
          A => {
            val x = A._4
            val u = A._6
            x + u
          }
        }.treeReduce(_ + _, 2)

        zprev := bc_z.value
        z := DenseVector(soft_threshold(new_z * (1.0 / num_part), lambda / (num_part * rho)))
        bc_z = cholesky_A.sparkContext.broadcast(z)


        prires = sqrt(recv(0)) /* sqrt(sum ||r_i||_2^2) */
        nxstack = sqrt(recv(1)) /* sqrt(sum ||x_i||_2^2) */
        nystack = sqrt(recv(2))
        /* sqrt(sum ||y_i||_2^2) */
        //primal and dual feasibility tolerance
        val eps_pri = sqrt(D * num_part) * absTol + relTol * max(nxstack, sqrt(num_part) * norm(z))
        val eps_dual = sqrt(D * num_part) * absTol + relTol * nystack
        val zdiff = z - zprev
        val dualres = sqrt(num_part) * rho * norm(zdiff)


        dataset.mapPartitionsWithIndex {
          (idx, iterator) => {
            val G = iterator.next
            val snp_matrix = G._1
            val b = G._2(::, Phe)
            println(idx, iter, prires, eps_pri, dualres, eps_dual, objective(snp_matrix, b, lambda, z))
          }
            Iterator(eps_pri)
        }

        if (prires <= eps_pri && dualres <= eps_dual) {
          break
        }

        iter += 1
      } //end while
    }
    //end breakable
    val elapsedTime = (System.nanoTime() - startTime) / 1e9

    println(s"Training time: $elapsedTime seconds")
    z.data.foreach(println)
  }

  def objective(A: DenseMatrix[Double], b: DenseVector[Double], lambda: Double, z: DenseVector[Double]) = {

    val Az = A * z
    val Azb = Az - b
    val Azb_nrm2 = Azb.data.map(x => x * x).reduce(_ + _)
    val obj = 0.5 * Azb_nrm2 + lambda * z.data.map(x => abs(x)).reduce(_ + _)
    obj
  }

  def soft_threshold(v: DenseVector[Double], k: Double) = {
      v.data.map { vi =>
        {
          if (vi > k) vi - k
          else if (vi < -k) vi + k
          else 0
        }
      }
  } //end soft_threshold

  // solve Ax = b, for x, where A = choleskyMatrix * choleskyMatrix.t
  // choleskyMatrix should be lower triangular
  def solve(choleskyMatrix: DenseMatrix[Double], b: DenseVector[Double], x: DenseVector[Double]) = {
    val C = choleskyMatrix
    val size = C.rows
    if (C.rows != C.cols) {
      // throw exception or something
    }
    if (b.length != size) {
      // throw exception or something
    }
    // first we solve C * y = b
    // (then we will solve C.t * x = y)
    val y = DenseVector.zeros[Double](size)
    // now we just work our way down from the top of the lower triangular matrix
    for (i <- 0 until size) {
      var sum = 0.0
      for (j <- 0 until i) {
        sum += C(i, j) * y(j)
      }
      y(i) = (b(i) - sum) / C(i, i)
    }
    // now calculate x
    //val x = DenseVector.zeros[Double](size)
    val Ct = C.t
    // work up from bottom this time
    for (i <- size - 1 to 0 by -1) {
      var sum = 0.0
      for (j <- i + 1 until size) {
        sum += Ct(i, j) * x(j)
      }
      x(i) = (y(i) - sum) / Ct(i, i)
    }

  }

}
