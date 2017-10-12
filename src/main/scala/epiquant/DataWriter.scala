package epiquant

import java.io.{File, PrintWriter}
import scala.io.Source


object DataWriter {

  def write(snp_f: String, phe_f: String, out_f: String): Int = {

    val dest = new PrintWriter(new File(out_f))

    val snp_src = Source.fromFile(snp_f)
    val phe_src = Source.fromFile(phe_f)

    val snp_arr: Array[Array[String]] = snp_src.getLines().map(_.split("\t")).toArray
    val phe_iter: Iterator[Array[String]] = phe_src.getLines().map(_.split("\t"))

//  keep the headers
    val sample_names = snp_arr(0).drop(5)
    val snp_names = snp_arr.drop(1).map(x => x(0))
    val phe_names = phe_iter.next().drop(1)
    //val phe_names = phe_iter(0).drop(1)

// transposed double array
    val snp_arr_t = snp_arr.drop(1).map(x => x.drop(5).map(_.toDouble)).transpose
    var i = 0
    val phe_cols = phe_names.length

    val snp_cols = snp_names.length
    val dim_X = snp_cols * (snp_cols+1) / 2

    for (line <- snp_arr_t) {
      val phe_line: Array[String] = phe_iter.next().drop(1)
      dest.write(phe_line.mkString("\t") + "\t")
      dest.print(line.mkString("\t") + "\t" )
      for (k <- 0 until snp_cols - 1) {
        for (j <- k + 1 until snp_cols) {
          if (k==snp_cols-2 && j == snp_cols-1) {
            //print (k)
            dest.println((line(k) * line(j)).toString())
          } else {
            dest.write((line(k) * line(j)).toString() + '\t')
          }
        }
      }
      i = i + 1
    }

    snp_src.close()
    phe_src.close()
    dest.close()

    phe_cols
  }

}

