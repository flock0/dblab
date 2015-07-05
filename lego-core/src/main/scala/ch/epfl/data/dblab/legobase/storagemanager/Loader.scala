package ch.epfl.data
package dblab.legobase
package storagemanager

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import schema._
import sc.pardis.shallow.OptimalString
import sc.pardis.types._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

@metadeep(
  folder = "",
  header = """import ch.epfl.data.dblab.legobase.deep._
import ch.epfl.data.dblab.legobase.deep.queryengine._
import ch.epfl.data.dblab.legobase.schema._
import scala.reflect._
""",
  component = "",
  thisComponent = "ch.epfl.data.dblab.legobase.deep.DeepDSL")
class MetaInfo

@needs[(LegobaseScanner, Array[_], OptimalString)]
@deep
trait Loader

/**
 * A module that defines loaders for relations.
 */
object Loader {

  @dontInline
  def getFullPath(fileName: String): String = Config.datapath + fileName

  def loadString(size: Int, s: LegobaseScanner) = {
    val NAME = new Array[Byte](size + 1)
    s.next(NAME)
    new OptimalString(NAME.filter(y => y != 0))
  }

  @dontInline
  def fileLineCount(file: String) = {
    import scala.sys.process._;
    Integer.parseInt(((("wc -l " + file) #| "awk {print($1)}").!!).replaceAll("\\s+$", ""))
  }

  @dontInline
  def loadTable(table: Table): Array[LegobaseRecord] = {
    val size = fileLineCount(table.resourceLocator)
    val arr = new Array[LegobaseRecord](size)
    val ldr = new LegobaseScanner(table.resourceLocator)

    var i = 0

    val argNames = table.attributes.map(_.name).toSeq

    while (i < size && ldr.hasNext()) {
      val values = table.attributes.map(arg =>
        arg.dataType match {
          case IntType          => ldr.next_int
          case DoubleType       => ldr.next_double
          case CharType         => ldr.next_char
          case DateType         => ldr.next_date
          case VarCharType(len) => loadString(len, ldr)
        })
      val rec = new LegobaseRecord(table.attributes.map(_.name) zip values)
      arr(i) = rec
      i += 1
    }
    arr

    //TODO update statistics
  }
}