object Starter {
  def main(args: Array[String]): Unit = {
    if (args.length != 2 || args.length != 3) {
      print("Not Enough Arguments! ")
      return
    }
    val snp_f = args(0)
    val phe_f = args(1)
    val out_f = if (args(2) == null) "out.txt" else args(2)

    print(epiquant.DataWriter.write(snp_f, phe_f, out_f)) 
  }
}
