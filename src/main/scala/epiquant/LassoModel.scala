package epiquant

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.Saveable
import org.apache.spark.mllib.regression.impl.GLMRegressionModel
import org.apache.spark.mllib.linalg.{Vector => V, DenseVector => DV, SparseVector => SV}
import org.apache.spark.mllib.util.Loader


object VectorConverter {
  /**
    * Creates a vector instance from a breeze vector.
    */
  def fromBreeze(breezeVector: BV[Double]): V = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1 && v.length == v.data.length) {
          new DV(v.data)
        } else {
          new DV(v.toArray)  // Can't use underlying array directly, so make a new one
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SV(v.length, v.index, v.data)
        } else {
          new SV(v.length, v.index.slice(0, v.used), v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }
}

class LassoModel(weights: BV[Double], itcpt: Double, names: Vector[String]) extends Serializable {
  //extends GeneralizedLinearModel(VectorConverter.fromBreeze(weights), itcpt)
  //with Serializable with Saveable {

  val intercept: Double = itcpt;
  val coefficients: BV[Double] = weights;
  val features: Vector[String] = names;
  val format: String = "1.0";
  val trainingTime: String = "0.0s";
  val mse: Double = 0.0;

  def predict(testData: BV[Double]): Double = {
    return (testData dot coefficients) + intercept
  }

 // override def predict(testData: RDD[V]): RDD[Double] = super.predict(testData)

  //override def predict(testData: V): Double = super.predict(testData)

  //override def predictPoint(dataMatrix: V, weightMatrix: V, intercept: Double): Double = ???

  def formatVersion: String = {
    return format;
  }

  def save(sc: SparkContext, path: String): Unit = {
    def thisFormatVersion: String = "1.0"
    val modelRDD = sc.parallelize(Seq("Coefficients" + intercept.toString + coefficients.toString))
    val a = coefficients.toArray.zip(features).filter(_._1 != 0)
    val featuresRDD = sc.parallelize(Seq(a));
    val evalRDD = sc.parallelize(Seq("MSE: " + mse + "\n" + "Training Time: " + trainingTime));
    featuresRDD.saveAsTextFile(path + "/coef.txt")
    evalRDD.saveAsTextFile(path + "/eval.txt")
    //GLMRegressionModel.SaveLoadV1_0.save(sc, path, this.getClass.getName, weights, intercept)
  }




}

/*object LassoModel extends Loader[LassoModel] {
   def load(sc: SparkContext, path: String) : LassoModel = {
    val (loadedClassName, version, metadata) = Loader.loadMetadata(sc, path)
    // Hard-code class name string in case it changes in the future
    val classNameV1_0 = "org.apache.spark.mllib.regression.LassoModel"
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val numFeatures = RegressionModel.getNumFeatures(metadata)
        val data = GLMRegressionModel.SaveLoadV1_0.loadData(sc, path, classNameV1_0, numFeatures)
        new LassoModel(VectorConverter.toBreeze(data.weights), data.intercept)
      case _ => throw new Exception(
        s"LassoModel.load did not recognize model with (className, format version):" +
        s"($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }

  def save(
    sc: SparkContext,
    path: String,
    modelClass: String,
    weights: V,
    intercept: Double): Unit = {
    val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

    // Create JSON metadata.
    val metadata = compact(render(
      ("class" -> modelClass) ~ ("version" -> thisFormatVersion) ~
        ("numFeatures" -> weights.size)))
    sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

    // Create Parquet data.
    val data = Data(weights, intercept)
    spark.createDataFrame(Seq(data)).repartition(1).write.parquet(Loader.dataPath(path))
  }


}*/
