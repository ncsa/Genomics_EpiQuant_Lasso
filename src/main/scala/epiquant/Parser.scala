package epiquant

package epiquant

import scopt.OptionParser

import scala.io.Source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

import scala.reflect.runtime.universe._
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.rdd.RDD


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

  def createParser() = {
    val parser = new OptionParser[Params]("epiquant") {
      head("EPIQUANT: an statsistical analysis for GWAS data in SPARK/scala.")
      opt[String]('s', "snpFile")
        .valueName("<snp file>")
        .text(s"input path to file with SNP values for individuals")
        .required
        .action((x, c) => c.copy(snpFile = x))
      opt[String]('p', "pheFile")
        .valueName("<phenotype file>")
        .text(s"input path to file specifying the phenotypic data for individuals")
        .required
        .action((x, c) => c.copy(phenotypeFile = x))
      opt[String]('o', "outFile")
        .valueName("<output file>")
        .text(s"path to output file")
        .action((x, c) => c.copy(output = x))

      opt[String]("cols_1").abbr("c1")
        .valueName("\"1 2 3...\"")
        .text(s"Unwanted columns in snpFile, zero indexed, separated by space.")
        .action((x, c) => c.copy(snpRmCols = x.split(" ").map(y => y.toInt)))
      opt[String]("cols_2").abbr("c2")
        .valueName("\"1 2 3...\"")
        .text(s"Unwanted columns in pheFile, zero indexed, separated by space.")
        .action((x, c) => c.copy(pheRmCols = x.split(" ").map(y => y.toInt)))

      opt[Unit]("snpFile transpose")
        .abbr("t1")
        .text(s"whether the snpFile data matrix needs transpose. Default: false")
        .action((x, c) => c.copy(t1 = true))
      opt[Unit]("pheFile transpose")
        .abbr("t2")
        .text(s"whether the pheFile data matrix needs transpose. Default: false")
        .action((x, c) => c.copy(t2 = true))
      opt[Double]('a', "alpha")
        .text(s"significance level: a threshold value for p-value test. " +
          s"Default: 0.05")
        .action((x, c) => c.copy(significance = x))
      checkConfig { params =>
        if (params.significance < 0 || params.significance >= 1) {
          failure(s"fracTest ${params.significance} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }
    parser
  }


  def parse(spark: SparkSession, args: Array[String]): (RDD[(BDM[Double], BDM[Double], Int)], Array[String], Array[String], Array[String]) = {
    val snp_f = args(0)
    val phe_f = args(1)


    val data: Array[Array[String]] = Source.fromFile(snp_f).getLines().map(_.split("\t")).toArray
     val samples: Array[String] = data(0).drop(5)
     val snp_names = data.drop(1).map(x => x(0))




    /*val data: RDD[Array[Array[String]]] = spark.sparkContext.textFile(snp_f)
      .map(_.split("\n").map(_.split("\t")))
    val samples: RDD[Array[String]] = data.map(array => array(0).drop(5))
    val snp_names: RDD[Array[String]] = data.map(array => array.drop(1).map(x => x(0)))*/



    val phefile = Source.fromFile(phe_f).getLines().map(_.split("\t")).toArray
    val phe_samples = phefile.drop(1).map(x => x(0))
    val phe_names = phefile(0).drop(1)

    val phe_vales_array: Array[Array[Double]] = phefile.drop(1).map {
      x => x.drop(1).map(_.toDouble)
    }

    val snp_darray: Array[Array[Double]] = data.drop(1).map(x => x.drop(5).map(_.toDouble)).transpose

    val stick = for (i <- snp_darray.indices) yield {
      (snp_darray(i), phe_vales_array(i))
    }

    val rdd = spark.sparkContext.parallelize(stick)
    val part = rdd.getNumPartitions

    //construct the matrix snp_matrix(i)
    val values = rdd.mapPartitionsWithIndex {
      (indx, x) => {
        val snp_1: Array[Array[Double]] = new Array[Array[Double]](samples.length / part + 1)
        val phe_2: Array[Array[Double]] = new Array[Array[Double]](samples.length / part + 1)
        val n = snp_names.length
        var num_rows = 0

        while (x.hasNext) {
          val temp = x.next
          snp_1(num_rows) = temp._1
          phe_2(num_rows) = temp._2
          num_rows += 1
        }

        //TODO: unnecessary reconstruction of phe_matrix. see if can construct in previous loop.
        val snp_matrix = BDM.zeros[Double](num_rows, (n * (n + 1) / 2))
        val phe_matrix = BDM.zeros[Double](num_rows, phe_names.length)

        for (k <- 0 until num_rows) {
          var c = n
          for (i <- 0 until n - 1) {
            for (j <- i + 1 until n) {
              snp_matrix(k, c) = snp_1(k)(i) * snp_1(k)(j)
              c += 1
            }
          }
          for (i <- phe_names.indices) {
            phe_matrix(k, i) = phe_2(k)(i)
          }
        }

        Iterator((snp_matrix, phe_matrix, num_rows))
      }
    }.persist(storage.StorageLevel.MEMORY_AND_DISK)

    (values, snp_names, phe_names, samples)

  }


  def CreateDataframe(spark: SparkSession, file: String, t: Boolean, rmCols: Array[Int]): DataFrame = {

    val fileRDD = spark.sparkContext.textFile(file)

    /* filter out the unnecessary columns as supplied in the main arguments */

    val splitRDD = if (rmCols != null) {
      fileRDD.map {
        line =>
          line.split("\t").zipWithIndex
            .filterNot(x => rmCols.contains(x._2)).map(x => x._1)
      }
    } else fileRDD.map(line => line.split("\t"))

    /* transpose the data if necessary */
    val finalData =
      if (t) {
        val byColumnAndRow = splitRDD.zipWithIndex.flatMap {
          case (row, rowIndex) => row.zipWithIndex.map {
            case (string, columnIndex) => columnIndex -> (rowIndex, string)
          }
        }
        val byColumn = byColumnAndRow.groupByKey.sortByKey().values
        byColumn.map {
          indexedRow => indexedRow.toArray.sortBy(_._1).map(_._2)
        }
      } else {
        splitRDD
      }

    /* filter out header row */

    val header = finalData.first()
    val filtered = finalData.mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)

    /* turn filtered data into Rows, where in each Row
     * the first element is a String (the distinct identifier for individuals)
     * followed by Doubles (actual data for each SNP)
     */

    val rowRDD = filtered.map {
      attributes =>
        Row.fromSeq(attributes(0)
          +: attributes.slice(1, attributes.length).map(x => x.toDouble))
    }

    /* build schema: the first column is of StringType and the rest of columns are DoubleType
     * corresponding to the RDD of Rows int the previous step
     */

    val schema = StructType(header
      .map(fieldName => StructField(fieldName, if (fieldName == header(0)) StringType else DoubleType, nullable = false)))

    spark.createDataFrame(rowRDD, schema)
  } //end CreateDataFrame
}
//end class
