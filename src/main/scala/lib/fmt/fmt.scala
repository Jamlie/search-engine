package lib.fmt

import java.io.PrintStream

object fmt {
  def print[T](x: T): Unit = fprintf(System.out, x)

  def println(): Unit = scala.Predef.println()

  def println[T](x: T): Unit = fprintlnf(System.out, x)

  def eprint[T](x: T): Unit = fprintf(System.out, x)

  def eprintln(): Unit = System.err.println()

  def eprintln[T](x: T): Unit = fprintlnf(System.err, x)

  private def fprintf[T](printStream: PrintStream, x: T): Unit = {
    x match {
      case arr: Array[_] => {
        val elements = arr.mkString(", ")
        printStream.print(s"[$elements]")
      }
      case _ => printStream.print(x)
    }
  }

  private def fprintlnf[T](printStream: PrintStream, x: T): Unit = {
    x match {
      case arr: Array[_] => {
        val elements = arr.mkString(", ")
        printStream.println(s"[$elements]")
      }
      case _ => printStream.println(x)
    }
  }
}
