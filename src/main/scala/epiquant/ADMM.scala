package epiquant

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.storage._
import org.apache.spark.broadcast._
import breeze.linalg._

import scala.util.control._
import epiquant.LassoModel




 class ADMM (val geno_data: Data, val pheno_data: Data, @transient val sc: SparkContext)
  extends Serializable {
  // Minimize
  //     loss(A * x, b) + lambda * ||x||_1
  // <=> \sum loss(A_i * x_i, b_i) + lambda * ||z||_1
  //      s.t x_i - z = 0
    require(geno_data.num_samples == pheno_data.num_samples, "Genotype and Phenotype files must be match.")

   //val data = geno.join(pheno).map { f => new DenseVector[Double](f._2._1 ++ f._2._2) }
    val data = geno_data.data.join(pheno_data.data).map { f => new DenseVector[Double](f._2._1 ++ f._2._2) }

    val snp_length = geno_data.num_features
    val phe_length = pheno_data.num_features
    val sample_length = geno_data.num_samples
    private val npart = data.getNumPartitions

     val cached: RDD[(DenseMatrix[Double], DenseVector[Double])] =  data.mapPartitions { p =>
       val matrix = p.foldLeft(DenseMatrix.zeros[Double](0,snp_length + phe_length)) {
         (mat, vec) => {
           DenseMatrix.vertcat(mat, vec.asDenseMatrix)
         }
       }
       val A = matrix(::, 0 to snp_length - 1)
       //val B = matrix(::, -phe_length to -1)
       val b = matrix(::, -1)

       //assuming AtA is positive definite
       val L = cholesky(A.t * A + rho * DenseMatrix.eye[Double](A.cols)) // AtA = LLt
       val Atb = A.t * b

       Seq((L, Atb)).iterator
     }.cache()

   val L = cached.map(x => x._1)
   val Atb = cached.map(x => x._2)

    // Penalty parameter
    protected var lambda: Double = 1.0
    // Parameters related to convergence
    private var max_iter: Int = 1000
    private var tol_abs: Double = 1e-6
    private var tol_rel: Double = 1e-3
    protected var rho: Double = 1.0

    var logs: Boolean = false


    // Main variable

    var admm_x = data.mapPartitions{ p => Seq(DenseVector.zeros[Double](snp_length)).iterator }
    admm_x.cache()
    var xbar = DenseVector.zeros[Double](snp_length)

    // Auxiliary variable
    var zbar: SparseVector[Double] = new VectorBuilder[Double](snp_length).toSparseVector
    var zbroad : Broadcast[SparseVector[Double]] = sc.broadcast(zbar)

    // Dual variable
    var admm_u = data.mapPartitions{ p => Seq(DenseVector.zeros[Double](snp_length)).iterator  }
    var ubar = DenseVector.zeros[Double](snp_length) //Array.fill(snp_length)(0.0)

    // Number of iterations
    private var iter = 0
    // Residuals and tolerance
    var eps_primal = 0.0;
    var eps_dual = 0.0;
    var resid_primal = 0.0;
    var resid_dual = 0.0;
    var resid = admm_x.map(x => DenseVector.zeros[Double](snp_length))

    var mse = 0.0

    // Soft threshold S (lambda/(rho*N)) (xbar^(k+1) + ubar^(k))
    //          a−κ,  a>κ
    // Sκ(a) =  0,   |a| ≤ κ
    //          a+κ,  a< −κ
    private def soft_threshold(vec: DenseVector[Double], penalty: Double): SparseVector[Double] = {
        val builder = new VectorBuilder[Double](vec.size)
        for (idx <- 0 until vec.size) {
          val v = vec(idx)
          if (v > penalty) {
            builder.add(idx, v - penalty)
          } else if (v < -penalty) {
            builder.add(idx, v + penalty)
          }
        }
        builder.toSparseVector(true, true)
      }


    def meanSquaredError(A: DenseMatrix[Double], y: DenseVector[Double], predict: DenseVector[Double]): Double = {
        val y_predict = A*predict
        val error = y_predict - y
        return (error dot error)/error.length
    }


    // Convenience functions
    private def square(x: Double): Double = x * x
    private def max2(x: Double, y: Double): Double = if(x > y) x else y

    // Tolerance for primal residual
   // ε^pri = √p ε^abs + ε^rel max{∥Axk∥2,∥Bzk∥2,∥c∥2},
    private def compute_tol_primal(): Double = {
     //val r = Math.max(norm(xbar), norm(zbar))
     val xsqnorm = admm_x.map(x => square(norm(x))).sum()
     val r = Math.max(Math.sqrt(xsqnorm), norm(zbar)* Math.sqrt(npart))
      return r * tol_rel + Math.sqrt(snp_length * npart) * tol_abs
    }
    // Tolerance for dual residual
    // ε^dual = √n ε^abs + εrel ∥At y^k∥2,
    private def compute_tol_dual(): Double = {
      val ysqnorm = admm_u.map(x => square(norm(x))).sum()
      //val r = norm(ubar * rho)
      val r = Math.sqrt(ysqnorm)
      return r * tol_rel + Math.sqrt(snp_length * npart) * tol_abs
    }

   // Tolerance for primal residual
   private def compute_eps_primal(): Double = {
     val xsqnorm = admm_x.map(x => square(norm(x))).sum()
     val r = max2(math.sqrt(xsqnorm), norm(zbar) * math.sqrt(npart))
     return r * tol_rel + math.sqrt(snp_length * npart) * tol_abs
   }
   // Tolerance for dual residual
   private def compute_eps_dual(): Double = {
     val ysqnorm = admm_u.map(x => square(norm(x))).sum()
     return math.sqrt(ysqnorm) * tol_rel + math.sqrt(snp_length * npart) * tol_abs
   }

    // Primal residual
    //r^(k+1) = Ax^(k+1) + Bz^(k+1) − c
   private def compute_resid_primal(): Double = {
      val r_i = admm_x.map(x => x - zbar)
      val r = math.sqrt(r_i.map(x => square(norm(x))).sum())
      //val r_i = admm_x.map(x => Math.pow(norm(x - zbroad.value), 2))
      //val r = Math.sqrt(r_i.reduce(_+_))
      return r
    }

    // Dual residual
    // s^(k+1) = ρAtB(z^(k+1) − z^k)
    private def compute_resid_dual(): Double = {
      rho * Math.sqrt(npart) * norm(zbar - zbroad.value)
    }

   protected def logging(iter: Int): Unit = {
     if (iter == 0)
       println("  #     r-norm     eps_pri     s-norm     eps_dual     objective");
     println(f"$iter%3d $resid_primal%10.4f $eps_primal%10.4f $resid_dual%10.4f $eps_dual%10.4f\n"); //$objective(A, b, lambda,z)%10.4f
   }

    def set_opts(max_iter: Int = 1000, eps_abs: Double = 1e-6, eps_rel: Double = 1e-6,
                 rho: Double = 1, lambda: Double = 0.5, logs: Boolean = false) {
      this.max_iter = max_iter
      this.tol_abs = eps_abs
      this.tol_rel = eps_rel
      this.rho = rho
      this.lambda = lambda
      this.logs = logs
    }
   // Update x
   // x-update: admm_x = (A^T A + rho I) \ (A^T b + rho (z - u))
   /* if skinny: x = U \ (L \ q) */
   /* else x = q/rho - 1/rho^2 * A^T * (U \ (L \ (A*q))) */
   def update_x(): Unit = {
     val admm_x_new = admm_u.zipPartitions(L, Atb, preservesPartitioning = true) {
       (uIter, LIter, AtbIter) => {
         val q = AtbIter.next + rho * (zbar - uIter.next)
         val res = Solver.dpotrs(LIter.next, q)
         Seq(res).iterator
       }
     }

     xbar  = admm_x_new.reduce(_+_)/npart.toDouble
     admm_x = admm_x_new
     admm_x.checkpoint()
   }

   def update_u(): Unit = {
     val new_admm_u = admm_u.zipPartitions(admm_x, preservesPartitioning = true) {
       (uIter, xIter) => {
         val newu = uIter.next() + xIter.next() - zbroad.value
         Seq(newu).iterator
       }
     }
     ubar  = new_admm_u.reduce(_+_)/npart.toDouble
     admm_u = new_admm_u
     admm_u.checkpoint()
   }

   def update_z(): SparseVector[Double] = {
     return soft_threshold((xbar + ubar), lambda / (rho * npart))
   }

  /* def run() {
     val loop = new Breaks
     loop.breakable {
       for (i <- 0 until max_iter) {

         val new_admm_u = admm_u.zipPartitions(admm_x, preservesPartitioning = true) {
           (uIter, xIter) => {
             val newu = uIter.next() + xIter.next() - zbroad.value
             Seq(newu).iterator
           }
         }
         admm_u = new_admm_u
         admm_u.checkpoint()


         val admm_x_new = admm_u.zipPartitions(L, Atb, preservesPartitioning = true) {
           (uIter, LIter, AtbIter) => {
             val q = AtbIter.next + rho * (zbroad.value - uIter.next)
             val res = Solver.dpotrs(LIter.next, q)
             Seq(res).iterator
           }
         }
         admm_x = admm_x_new
         admm_x.checkpoint()

         xbar  = admm_x_new.reduce(_+_)/npart.toDouble

         resid_primal = math.sqrt(resid.map(ri => square(norm(ri))).sum())

         val new_z = soft_threshold((xbar + ubar), lambda / (rho * npart))
         resid_dual = rho * Math.sqrt(npart) * norm(new_z - zbroad.value)
         zbroad = sc.broadcast(new_z)


         // Calculate tolerance values

         resid_primal = math.sqrt(resid.map(x => square(norm(x))).sum())


         val ysqnorm = admm_u.map(u => square(norm(u))).sum()
         eps_dual = math.sqrt(ysqnorm) * tol_rel + math.sqrt(snp_length * npart) * tol_abs

         val xsqnorm = admm_x.map(x => square(norm(x))).sum()
         val r = math.max(math.sqrt(xsqnorm), norm(zbroad.value) * math.sqrt(npart))
         eps_primal = r * tol_rel + math.sqrt(snp_length * npart) * tol_abs


         iter = i
         if (logs)
           logging(iter)
         // Convergence test
         if (resid_primal < eps_primal && resid_dual < eps_dual) {
           loop.break
         }
       }
     }
   }

   def coef = zbroad.value.copy
   def niter = iter
 }*/



   def run() {
      val loop = new Breaks
      loop.breakable {
        for (i <- 0 until max_iter) {
          val ysqnorm = admm_u.map(u => square(norm(u))).sum()
          eps_dual = math.sqrt(ysqnorm) * tol_rel + math.sqrt(snp_length * npart) * tol_abs

          val xsqnorm = admm_x.map(x => square(norm(x))).sum()
          val r = math.max(math.sqrt(xsqnorm), norm(zbar) * math.sqrt(npart))

          eps_primal = r * tol_rel + math.sqrt(snp_length * npart) * tol_abs
          val admm_x_new = admm_u.zipPartitions(L, Atb, preservesPartitioning = true) {
            (uIter, LIter, AtbIter) => {
              val q = AtbIter.next + rho * (zbar - uIter.next)
              val res = Solver.dpotrs(LIter.next, q)
              Seq(res).iterator
            }
          }
          admm_x = admm_x_new
          admm_x.checkpoint()

          xbar  = admm_x_new.reduce(_+_)/npart.toDouble
          ubar = admm_u.reduce(_+_)/npart.toDouble

          val new_z = soft_threshold((xbar + ubar), lambda / (rho * npart))
          resid_dual = rho * Math.sqrt(npart) * norm(new_z - zbar)
          zbar = new_z
          zbroad = sc.broadcast(zbar)


          // Calculate tolerance values
          val resid = admm_x.map(x => x - zbroad.value)
          resid.cache()
          resid_primal = math.sqrt(resid.map(x => square(norm(x))).sum())


          val new_admm_u = admm_u.zipPartitions(admm_x, preservesPartitioning = true) {
            (uIter, xIter) => {
              val newu = uIter.next() + xIter.next() - zbroad.value
              Seq(newu).iterator
            }
          }
          admm_u = new_admm_u
          admm_u.checkpoint()


          iter = i
          if (logs)
            logging(iter)
          // Convergence test
          if (resid_primal < eps_primal && resid_dual < eps_dual) {
            loop.break
          }
        }
      }
    }

    def coef = zbar.copy
    def niter = iter
}

/*val A =  data.mapPartitions { p =>
  val matrix = p.foldLeft(DenseMatrix.zeros[Double](0, 11)) {
    (mat, vec) => {
      DenseMatrix.vertcat(mat, vec.asDenseMatrix)
    }
  }
  val A = matrix(::, 0 to snp_length-1)
  val B = matrix(::, -phe_length to -1)
  val b = matrix(::, -1)
  Seq(A).iterator
}
//A.saveAsTextFile("output/A_10x20.txt")


val B = data.mapPartitions { p =>
  val matrix = p.foldLeft(DenseMatrix.zeros[Double](0, 11)) {
    (mat, vec) => {
      DenseMatrix.vertcat(mat, vec.asDenseMatrix)
    }
  }
  val b = matrix(::, -1)
  Seq(b).iterator
}

val AtA = A.map(x => x.t * x)


//val L = cached.map(x => x._1)
//val Atb = cached.map(x => x._2)
//val AtApI = AtA.map( x => x + DenseMatrix.eye[Double](x.rows))

cached.foreach{ x =>
  println(x._1)
  println(x._2)
}
*/

/*
// Calculate tolerance values
eps_primal = compute_tol_primal()
eps_dual = compute_tol_dual()
update_x()
admm_x.checkpoint()
val new_z = update_z()
resid_dual = compute_resid_dual(new_z)
zbar = new_z
zbroad = sc.broadcast(zbar)
resid_primal = norm(xbar - zbar)


// u step
update_u()

iter = i
*/