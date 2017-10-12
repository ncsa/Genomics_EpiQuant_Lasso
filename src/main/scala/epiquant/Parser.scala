package epiquant


//import scopt.OptionParser
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._
import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD


object Parser {

  case class Params(
                     snpFile: String = null,
                     phenotypeFile: String = null,
                     output: String = null,
                     snpRmCols: Array[Int] = null,
                     pheRmCols: Array[Int] = null,
                     t1: Boolean = false,
                     t2: Boolean = false,
                     significance: Double = 0.05
                   )

  val defaultParams = Params()

  def parse(spark: SparkSession, file: String, phe_length: Int): RDD[(DenseVector[Double], DenseMatrix[Double])] = {
    val txt: RDD[String] = spark.sparkContext.textFile(file, 10)
    val train: RDD[(DenseVector[Double], DenseMatrix[Double])] = {
      txt.mapPartitions(x => Array[(DenseVector[Double], DenseMatrix[Double])](read_data(x, phe_length)).iterator)
    }

    train
  }

  def read_data(txt: Iterator[String], phe_length: Int): (DenseVector[Double], DenseMatrix[Double]) = {
    val parsed = txt.map(x => x.split(' ').map(_.toDouble)).toArray

    //TODO: making it a loop
    //for (i <- 0 until phe_length) {
    //val y: Array[Double] = parsed.map(x => x(0))
    //new DenseVector(y)
    //}
    val y: Array[Double] = parsed.map(x => x(0))
    val n = parsed.length
    val p = parsed(0).length - phe_length
    val x = parsed.map(x => x.drop(phe_length)).flatMap(x => x)
    val mat_x = new DenseMatrix(p, n, x)

    (new DenseVector(y), mat_x.t.copy)
  }

}

//end class

abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}