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


    val sample_names = snp_arr(0).drop(5)
    val snp_names = snp_arr.drop(1).map(x => x(0))
    val phe_names = phe_iter.next()
    //val phe_names = phe_iter(0).drop(1)

    val snp_arr_t: Array[Array[String]] = snp_arr.drop(1).map(x => x.drop(5)).transpose

    var i = 0
    var phe_length = 0
    for (line <- snp_arr_t) {
      val phe_line: Array[String] = phe_iter.next().drop(1)
      phe_length = phe_line.length
      dest.write(phe_line.mkString("\t") + "\t")
      dest.println(line.mkString("\t"))
      i = i + 1
    }

    snp_src.close()
    phe_src.close()
    dest.close()

    phe_length
  }

}

