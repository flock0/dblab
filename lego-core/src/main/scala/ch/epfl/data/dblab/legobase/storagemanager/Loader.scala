package ch.epfl.data
package dblab.legobase
package storagemanager

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import tpch._
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

@needs[(K2DBScanner, Array[_], OptimalString)]
@deep
trait Loader

/**
 * A module that defines loaders for relations.
 */
object Loader {

  @dontInline
  def getFullPath(fileName: String): String = Config.datapath + fileName

  def loadString(size: Int, s: K2DBScanner) = {
    val NAME = new Array[Byte](size + 1)
    s.next(NAME)
    new OptimalString(NAME.filter(y => y != 0))
  }

  @dontInline
  def fileLineCount(file: String) = {
    import scala.sys.process._;
    Integer.parseInt(((("wc -l " + file) #| "awk {print($1)}").!!).replaceAll("\\s+$", ""))
  }

  /**
   * Loads a table from disk
   *
   * The order of the tuples' values on disk must be the same
   * as the constructor parameters for the passed class R and
   * the attributes in the passed table.
   *
   * @tparam R The class of the tuples to load
   * @param table The table to load data for
   * @return An array of tuples loaded from disk
   */
  @dontInline
  def loadTable[R](table: Table)(implicit c: ClassTag[R]): Array[R] = {
    val size = fileLineCount(table.fileName)
    val arr = new Array[R](size)
    val ldr = new K2DBScanner(table.fileName)
    val recordType = currentMirror.staticClass(c.runtimeClass.getName).asType.toTypeConstructor

    val classMirror = currentMirror.reflectClass(recordType.typeSymbol.asClass)
    val constr = recordType.decl(termNames.CONSTRUCTOR).asMethod
    val recordArguments = recordType.member(termNames.CONSTRUCTOR).asMethod.paramLists.head map {
      p => (p.name.decodedName.toString, p.typeSignature)
    }

    val arguments = recordArguments.map {
      case (name, tpe) =>
        (name, tpe, table.attributes.find(a => a.name == name) match {
          case Some(a) => a
          case None    => throw new Exception(s"No attribute found with the name `$name` in the table ${table.name}")
        })
    }

    var i = 0
    while (i < size && ldr.hasNext()) {
      val values = arguments.map(arg =>
        arg._3.dataType match {
          case IntType          => ldr.next_int
          case DoubleType       => ldr.next_double
          case CharType         => ldr.next_char
          case DateType         => ldr.next_date
          case VarCharType(len) => loadString(len, ldr)
        })

      classMirror.reflectConstructor(constr).apply(values: _*) match {
        case rec: R => arr(i) = rec
        case _      => throw new ClassCastException
      }
      i += 1
    }
    arr

    //TODO update statistics
  }

  /**
   * Loads a table into the in-memory DB
   *
   * @param catalog The catalog that should store the data
   * @param schemaName The name of the schema in the catalog
   * @param tableName The name of the table in the schema
   */
  def loadTable(catalog: Catalog, schemaName: String, tableName: String): Unit =
    loadTable(catalog, catalog.getTable(schemaName, tableName))

  /**
   * Loads a table into the in-memory DB
   *
   * @param catalog The catalog that should store the data
   * @param table The table to load into the catalog
   */
  def loadTable(catalog: Catalog, table: TablesRecord): Unit = {
    if (!table.isLoaded) {
      val fileName = table.fileName match {
        case Some(fn) => fn
        case None     => throw new Exception("No filename available in ${table.schemaName}.${table.name} to load data from")
      }
      val attributes = catalog.getAttributes(table.tableId)
      val size = fileLineCount(fileName)
      val ldr = new K2DBScanner(fileName)

      var i = 0
      while (i < size && ldr.hasNext()) {
        val values = attributes.map { at =>
          at.attributeId -> (at.dataType match {
            case IntType          => ldr.next_int
            case DoubleType       => ldr.next_double
            case CharType         => ldr.next_char
            case DateType         => ldr.next_date
            case VarCharType(len) => loadString(len, ldr)
          })
        }

        catalog.addTuple(table.tableId, values)

        i += 1
      }
      table.isLoaded = true
    }
  }

}