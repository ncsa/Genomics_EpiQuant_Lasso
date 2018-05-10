// Copyright Hugh Perkins 2012
// You can use this under the terms of the Apache Public License 2.0
// http://www.apache.org/licenses/LICENSE-2.0
package epiquant

import breeze.linalg._
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.netlib.util.intW

object Solver {
  /**
    * Triangular Cholesky solve for finding y through backsolves such that y := Ax
    *
    * @param A vector representation of lower triangular cholesky factorization
    * @param x the linear term for the solve which will also host the result
    */

    def dpotrs(A: DenseMatrix[Double], x: DenseVector[Double]): DenseVector[Double] = {
      val n = x.length
      val nrhs = 1
      require(A.rows == n)
      val info: intW = new intW(0)
      val xcopy = x.copy
      lapack.dpotrs("L", n, nrhs, A.data, 0, n, xcopy.data, 0, n, info)

      if (info.`val` > 0) throw new LapackException("DPOTRS : Leading minor of order i of A is not positive definite.")
      xcopy
    }



  @Deprecated
  def dtrsm(L: DenseMatrix[Double], x: DenseVector[Double]): DenseMatrix[Double] = {
    val Lcopy = L.copy
    val n = x.length
    val nrhs = 1
    val one = 1.0d + 0
    require(Lcopy.rows == n)
    require(Lcopy.rows == Lcopy.cols)
    val info: intW = new intW(0)
    blas.dtrsm("L", "L", "N", "N", n, nrhs, one, Lcopy.data, Math.max(1, n), x.data, Math.max(1, n))
    Lcopy
  }





  // solve Ax = b, for x, where A = choleskyMatrix * choleskyMatrix.t
  // choleskyMatrix should be lower triangular
  def solve( choleskyMatrix: DenseMatrix[Double], b: DenseVector[Double] ) : DenseVector[Double] = {
    val C = choleskyMatrix
    val size = C.rows
    if( C.rows != C.cols ) {
      // throw exception or something
      print("C Not factorizable!")
    }
    if( b.length != size ) {
      print("b Not factorizable!")
      // throw exception or something
    }
    // first we solve C * y = b
    // (then we will solve C.t * x = y)
    val y = DenseVector.zeros[Double](size)
    // now we just work our way down from the top of the lower triangular matrix
    for( i <- 0 until size ) {
      var sum = 0.0
      for( j <- 0 until i ) {
        sum += C(i,j) * y(j)
      }
      y(i) = ( b(i) - sum ) / C(i,i)
    }
    // now calculate x
    val x = DenseVector.zeros[Double](size)
    val Ct = C.t
    // work up from bottom this time
    for( i <- size -1 to 0 by -1 ) {
      var sum = 0.0
      for( j <- i + 1 until size ) {
        sum += Ct(i,j) * x(j)
      }
      x(i) = ( y(i) - sum ) / Ct(i,i)
    }
    x
  }
}
