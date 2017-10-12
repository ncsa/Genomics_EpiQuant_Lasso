package epiquant

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, storage}
import org.apache.spark.broadcast.Broadcast
import breeze.linalg.functions._
import breeze.optimize.proximal.QuadraticMinimizer

import scala.util.control.Breaks._
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector}
import breeze.linalg._
import breeze.optimize.proximal.QuadraticMinimizer
import breeze.numerics._
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.A
import org.apache.spark.sql.SparkSession._

import scala.util.control._

 class ADMM (val datx: RDD[DenseMatrix[Double]],
            val daty: RDD[DenseVector[Double]],
            @transient val sc: SparkContext)
  extends Serializable {
  // Minimize
  //     loss(A * x, b) + lambda * ||x||_1
  // <=> \sum loss(A_i * x_i, b_i) + lambda * ||z||_1
  //      s.t x_i - z = 0

    // Dimension of x
    private val dim_x = datx.first().cols
    // Number of partitions
    private val npart = datx.count()
    // Penalty parameter
    protected var lambda: Double = 0.0
    // Parameters related to convergence
    private var max_iter: Int = 100000
    private var eps_abs: Double = 1e-6
    private var eps_rel: Double = 1e-6

    protected var rho: Double = 1.0
    protected var broad_rho = sc.broadcast(rho)
    private var logs: Boolean = false

    // Main variable
    protected var admm_x: RDD[DenseVector[Double]] = datx.map(x => DenseVector.zeros[Double](x.cols))
    admm_x.cache()
    protected val xbar = DenseVector.zeros[Double](dim_x)

    // Auxiliary variable
    protected var admm_z = new VectorBuilder[Double](dim_x).toSparseVector
    protected var broad_z = sc.broadcast(admm_z)

    // Dual variable
    protected var admm_y: RDD[DenseVector[Double]] = datx.map(x => DenseVector.zeros[Double](x.cols))
    admm_y.cache()
    protected val ybar = DenseVector.zeros[Double](dim_x)
    // Number of iterations
    private var iter = 0
    // Residuals and tolerance
    protected var eps_primal = 0.0;
    protected var eps_dual = 0.0;
    protected var resid_primal = 0.0;
    protected var resid_dual = 0.0;

    // Soft threshold
    private def soft_shreshold(vec: DenseVector[Double], penalty: Double): SparseVector[Double] = {
      val builder = new VectorBuilder[Double](vec.size)
      for(ind <- 0 until vec.size) {
        val v = vec(ind)
        if(v > penalty) {
          builder.add(ind, v - penalty)
        } else if(v < -penalty) {
          builder.add(ind, v + penalty)
        }
      }
      builder.toSparseVector(true, true)
    }

    // Convenience functions
    private def square(x: Double): Double = x * x
    private def max2(x: Double, y: Double): Double = if(x > y) x else y

    // Tolerance for primal residual
    private def compute_eps_primal(): Double = {
      val xsqnorm = admm_x.map(x => square(norm(x))).sum()
      val r = max2(math.sqrt(xsqnorm), norm(admm_z) * math.sqrt(npart))
      return r * eps_rel + math.sqrt(dim_x * npart) * eps_abs
    }
    // Tolerance for dual residual
    private def compute_eps_dual(): Double = {
      val ysqnorm = admm_y.map(x => square(norm(x))).sum()
      math.sqrt(ysqnorm) * eps_rel + math.sqrt(dim_x * npart) * eps_abs
    }
    // Dual residual
    private def compute_resid_dual(new_z: SparseVector[Double]): Double = {
      rho * math.sqrt(npart) * norm(new_z - admm_z)
    }
    // Changing rho
    protected def rho_changed_action() {}
    private def update_rho() {
      var rho_changed = false

      if(resid_primal / eps_primal > 10 * resid_dual / eps_dual) {
        rho *= 2
        rho_changed = true
      } else if(resid_dual / eps_dual > 10 * resid_primal / eps_primal) {
        rho /= 2
        rho_changed = true
      }

      if(resid_primal < eps_primal) {
        rho /= 1.2
        rho_changed = true
      }

      if(resid_dual < eps_dual) {
        rho *= 1.2
        rho_changed = true
      }

      if(rho_changed) {
        rho_changed_action()
        broad_rho = sc.broadcast(rho)
      }
    }
    // Update x -- abstract method
    // minimize loss(x, y) + rho/2 * ||x - v||_1
    // x-update: admm_x = (A^T A + rho I) \ (A^T b + rho (z - y))
    /* if skinny: x = U \ (L \ q) */
    /* else x = q/rho - 1/rho^2 * A^T * (U \ (L \ (A*q))) */
    protected def update_x(x: DenseMatrix[Double], y: DenseVector[Double],
                           rho: Double, v: DenseVector[Double]): DenseVector[Double] = {

      //compute the matrix L : L = chol(I + 1/rho*AAt)
      //val AAt: DenseMatrix[Double] = x * x.t / rho
      val eye = DenseMatrix.eye[Double](x.rows)
      val L = cholesky(x * x.t / rho + eye) /* AAt+I = L*L^*.*/

      val q = Atb + v // q = (A^T b + rho z - y)
      val Aq = x*q
      val p = Solver.solve(L, Aq)      // p = (U \ (L \ (A*q))
      val ret = DenseVector.zeros[Double](x.cols)
      QuadraticMinimizer.gemv(1, x.t, p, 0, ret)  /* now ret = A^T * (U \ (L \ (A*q)) */
      q/rho - 1/square(rho) * ret   /* x = q/rho - 1/rho^2 * A^T * (U \ (L \ (A*q))) */
    }

    protected def Atb: DenseVector[Double] = datx.zip(daty).map {
      x => {
        x._1.t * x._2
      }
    }.reduce(_ + _) / npart.toDouble


   protected def logging(iter: Int) {}

    def set_opts(max_iter: Int = 1000, eps_abs: Double = 1e-6, eps_rel: Double = 1e-6,
                 rho: Double = 1, logs: Boolean = false) {
      this.max_iter = max_iter
      this.eps_abs = eps_abs
      this.eps_rel = eps_rel
      this.rho = rho
      broad_rho = sc.broadcast(rho)
      this.logs = logs
    }

    def set_lambda(lambda: Double) {
      this.lambda = lambda
    }

    def run() {
      val loop = new Breaks
      loop.breakable {
        for(i <- 0 until max_iter) {
          // Calculate tolerance values
          eps_primal = compute_eps_primal()
          eps_dual = compute_eps_dual()

          // x step
          admm_x = datx.zip(daty).zip(admm_y).map(t => update_x(t._1._1, t._1._2, broad_rho.value, (broad_z.value - t._2 ) * broad_rho.value))
          admm_x.cache()
          //admm_x.checkpoint()

          // z step
          xbar := admm_x.reduce(_ + _) / npart.toDouble
          ybar := admm_y.reduce(_ + _) / npart.toDouble
          val new_z = soft_shreshold(xbar + ybar / rho, lambda / (rho * npart))
          resid_dual = compute_resid_dual(new_z)
          admm_z = new_z
          broad_z = sc.broadcast(admm_z)

          // y step
          val resid = admm_x.map(x => x - broad_z.value)
          resid.cache()
          resid_primal = math.sqrt(resid.map(x => square(norm(x))).sum())
          admm_y = admm_y.zip(resid).map(t => t._1 + broad_rho.value * t._2)
          admm_y.cache()
          //admm_y.checkpoint()

          iter = i

          if(logs)
            logging(iter)

          // Convergence test
          if(resid_primal < eps_primal && resid_dual < eps_dual) {
            loop.break
          }

          if(i > 3)
            update_rho()
        }
      }
    }

    def coef = admm_z.copy
    def niter = iter

}
