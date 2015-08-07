package ch.epfl.data
package dblab.legobase
package utils

/**
 * A module containing useful utilities.
 */
object Utilities {
  def time[A](a: => A, msg: String) = {
    val start = System.nanoTime
    val result = a
    val end = (System.nanoTime - start) / (1000 * 1000)
    println(s"$msg completed in ${Console.BLUE}$end${Console.RESET} milliseconds")
    result
  }
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  def getNumLinesInFile(filePath: String): Int = {
    io.Source.fromFile(filePath).getLines.size
  }
}
