package epiquant


//import scopt.OptionParser

import scala.reflect.runtime.universe._
import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics._
import breeze.io.{CSVReader, CSVWriter, FileStreams}
import org.apache.spark.SparkContext


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

  def aggregate(merged: RDD[DenseVector[Double]]): DenseMatrix[Double] = {
    val seqop = (mat: DenseMatrix[Double], vec: DenseVector[Double]) => DenseMatrix.vertcat(mat, vec.asDenseMatrix)
    val combop = (acc1: DenseMatrix[Double], acc2: DenseMatrix[Double]) => DenseMatrix.vertcat(acc1, acc2)

    val d = merged.aggregate(DenseMatrix.zeros[Double](0,11))(seqop, combop)
    d
  }

  def aggregateByPartition(merged: RDD[DenseVector[Double]]): Unit = {
    val data = merged.mapPartitions{ p =>
      val matrix = p.foldLeft(DenseMatrix.zeros[Double](0,11)) {
        (mat, vec) => {
          DenseMatrix.vertcat(mat, vec.asDenseMatrix)
        }
      }
      p
    }
  }

  def parse2(sc: SparkContext, genofile: String, phenofile: String): (Data, Data) = {
    val geno_txt = sc.textFile(genofile, 1)
    val pheno_txt = sc.textFile(phenofile, 1)
    println(geno_txt.getNumPartitions)

    val geno = geno_txt.mapPartitionsWithIndex( (idx, iter) => if (idx == 0) iter.drop(1) else iter).map { str =>
        val arr = str.split("\t")
        (arr(0), arr.drop(1).map(_.toDouble))
      }.cache()

    val pheno = pheno_txt.mapPartitionsWithIndex( (idx, iter) => if (idx == 0) iter.drop(1) else iter).map { str =>
        val arr = str.split("\t")
        (arr(0), arr.drop(1).map(_.toDouble))
      }.cache()


    val snp_names: Array[String] = geno_txt.first.split("\t").drop(1)
    val pheno_names = pheno_txt.first.split("\t").drop(1)
    val phe_length = pheno_names.length
    val snp_length = snp_names.length

    val sample_length = geno.count().toInt


    val geno_data = new Data(geno, genofile, snp_length, sample_length, snp_names, sc)
    val pheno_data = new Data(pheno, phenofile, phe_length, sample_length, pheno_names, sc)

    (geno_data, pheno_data)
  }

  def parse(spark: SparkContext, file: String): RDD[(DenseVector[Double], DenseMatrix[Double])] = {
    val txt: RDD[String] = spark.textFile(file, 10).cache()

    val header = txt.first() //extract header
    val header_array = header.split("\t")
    val phe_length = header_array(0).toInt
    val snp_length = header_array(1).toInt
    val sample_length = header_array(2).toInt

    val txt_clean_1 = txt.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val snp_names = txt_clean_1.first()  //extract snp_names

    val data = txt_clean_1.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //rest are numeric values
    val parsed_data = data.map( row => {
      val explodedRow = row.split('\t').map(_.toDouble)
      explodedRow
    }
    ).cache()


    //val header = training_dat.first()
    //header
      //line => line.split("\t").map(_.toDouble))

    val train: RDD[(DenseVector[Double], DenseMatrix[Double])] = {
      data.mapPartitions(x => Array[(DenseVector[Double], DenseMatrix[Double])](read_data(x, phe_length)).iterator)
    }

    //val model = new ADMM(parsed_data, phe_length, snp_length, sample_length, snp_names.split("\t"), spark.sparkContext)
    return train
    //return model
  }

  def read_data(data: Iterator[String], phe_length: Int): (DenseVector[Double], DenseMatrix[Double]) = {
    val parsed: Array[Array[Double]] = data.map(x => x.split('\t').map(_.toDouble)).toArray
    //TODO: making it a loop
    //for (i <- 0 until phe_length) {
    //val y: Array[Double] = parsed.map(x => x(0))
    //new DenseVector(y)
    //}
    val y: Array[Double] = parsed.map(x => x(0))
    val n = parsed.length
    val p = parsed(0).length - phe_length
    val x: Array[Double] = parsed.map(x => x.drop(phe_length)).flatMap(x => x)
    val mat_x = new DenseMatrix[Double](p, n, x)

    (new DenseVector(y), mat_x.copy)
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