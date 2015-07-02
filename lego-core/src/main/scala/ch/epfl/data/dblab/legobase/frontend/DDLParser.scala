package ch.epfl.data
package dblab.legobase
package frontend

import schema._
import scala.language.postfixOps
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.syntactical._

/**
 * A DDL parser.
 * Based on: https://github.com/folone/DDL-comparer/blob/master/src/main/scala/DDLParser.scala with extensive modifications
 */
object DDLParser extends JavaTokenParsers {

  val tableName = """(?!(?i)KEY)(?!(?i)PRIMARY)(?!(?i)UNIQUE)(`)?[a-zA-Z_0-9]+(`)?""".r
  val schemaName = tableName
  val columnName = tableName
  val default = tableName
  val keyName = tableName

  // Handle comments
  protected override val whiteSpace = """(\s|--.*|#.*|(?m)/\*(\*(?!/)|[^*])*\*/;*)+""".r

  // DDL Attribute Types 
  sealed trait AttributeType
  final case class DDLInteger() extends AttributeType
  final case class DDLChar(numChars: Int) extends AttributeType
  final case class DDLVarChar(numChars: Int) extends AttributeType
  final case class DDLDecimal(integerDigits: Int, decimalPoints: Int) extends AttributeType
  final case class DDLDate() extends AttributeType

  def intType: Parser[DDLInteger] =
    "INTEGER" ^^ { case _ => new DDLInteger() }
  def charType: Parser[DDLChar] =
    "CHAR" ~ "(" ~> wholeNumber <~ ")" ^^ { case num => DDLChar(num.toInt) }
  def varcharType: Parser[DDLVarChar] =
    "VARCHAR" ~ "(" ~> wholeNumber <~ ")" ^^ { case num => DDLVarChar(num.toInt) }
  def decimalType: Parser[DDLDecimal] =
    "DECIMAL" ~ "(" ~> wholeNumber ~ "," ~ wholeNumber <~ ")" ^^ {
      case num ~ "," ~ num2 => DDLDecimal(num.toInt, num2.toInt)
    }
  def dateType: Parser[DDLDate] =
    "DATE" ^^ { case _ => new DDLDate() }
  def dataType: Parser[AttributeType] = intType | charType | varcharType | decimalType | dateType

  val statementTermination = ";"
  val columnDelimiter = """,*""".r

  final case class DDLTable(name: String, columns: Set[Column], constraints: Set[ConstraintOp])
  final case class Column(name: String, datatype: AttributeType, notNull: Boolean,
                          autoInc: Boolean, defaultVal: Option[String])
  sealed trait Constraint
  final case class DDLUniqueKey(table: String, uniqueCols: List[String]) extends Constraint
  final case class DDLCompressed(table: String, compressedCol: String) extends Constraint
  final case class DDLPrimaryKey(table: String, primaryKeyCols: List[String]) extends Constraint
  final case class DDLForeignKey(table: String, foreignKeyName: String, foreignKeyCols: List[(String, String)],
                                 foreignTable: String) extends Constraint

  def cleanString(str: String) = str.replaceAll("`", "")

  def column = columnName ~ dataType ~
    ("""(?i)NOT NULL""".r?) ~
    ("""(?i)AUTO_INCREMENT""".r?) ~
    ((("""(?i)DEFAULT""".r) ~ default)?) ~
    rep(annotations) ~
    columnDelimiter

  def uniqueOrPk = ("""(?i)(PRIMARY|UNIQUE)""".r?) ~ ("""(?i)KEY""".r) ~ "(" ~ repsep(columnName, ",") ~ ")" ~ columnDelimiter

  def fk = """FOREIGN KEY""".r ~ keyName ~ "(" ~ repsep(columnName, ",") ~ ")" ~ """(?i)REFERENCES""".r ~ tableName ~ "(" ~ repsep(columnName, ",") ~ ")" ~ columnDelimiter

  def constraint = (uniqueOrPk | fk)

  def compressed = "COMPRESSED"

  def annotations = compressed

  def createTable = ("""(?i)CREATE TABLE""".r) ~ ("""(?i)IF NOT EXISTS""".r?) ~ tableName ~
    "(" ~ (column*) ~ (constraint*) ~ ")" ^^ {
      case _ ~ _ ~ name ~ "(" ~ columns ~ cons ~ ")" => {
        val constraints = cons map {
          case (kind: Option[String]) ~ _ ~ "(" ~ (column: List[String]) ~ ")" ~ _ =>
            kind match {
              case Some(x) if x.equalsIgnoreCase("primary") => ConstraintOp(true, DDLPrimaryKey(name, column))
              case Some(x) if x.equalsIgnoreCase("unique")  => ConstraintOp(true, DDLUniqueKey(name, column))
            }
          case _ ~ (kn: String) ~ "(" ~ (localColumns: List[String]) ~ ")" ~ _ ~ tableName ~ _ ~ (foreignColumns: List[String]) ~ _ ~ _ => {
            //TODO Make sure that the two columns lists have the same size
            val matchedColumns = localColumns zip foreignColumns
            ConstraintOp(true, DDLForeignKey(name, kn, matchedColumns, tableName))
          }
        }

        val colsAndAnnos: Seq[(Column, Seq[ConstraintOp])] = columns map {
          case colName ~ colType ~ notNull ~ autoInc ~ isDefault ~ annotations ~ _ => {
            val anno = annotations map {
              case "COMPRESSED" => ConstraintOp(true, DDLCompressed(cleanString(name), colName))
            }
            (Column(cleanString(colName), colType, notNull.isDefined,
              autoInc.isDefined, isDefault.map(_._2)), anno)
          }
        }
        val (columnsData, annos) = colsAndAnnos.unzip
        DDLTable(cleanString(name), columnsData.toSet, (constraints ++ annos.flatten).toSet)
      }
    }

  trait CatalogOp
  final case class UseSchema(sName: String) extends CatalogOp
  final case class ConstraintOp(add: Boolean, cons: Constraint) extends CatalogOp
  final case class DropTable(tableName: String) extends CatalogOp

  def alterTableAddPrimary = ("ALTER TABLE" ~> tableName) ~ ("ADD" ~ "PRIMARY KEY (" ~> repsep(columnName, ",") <~ ")") ^^ {
    case tn ~ cols => ConstraintOp(true, DDLPrimaryKey(tn, cols))
  }
  def alterTableAddForeign = ("ALTER TABLE" ~> tableName) ~ ("ADD" ~ "FOREIGN KEY " ~> keyName) ~ ("(" ~> repsep(columnName, ",") <~ ")") ~ ("REFERENCES" ~> tableName) ~ ("(" ~> repsep(columnName, ",") <~ ")") ^^ {
    case tn ~ kn ~ localCols ~ ftn ~ foreignCols => {
      //TODO Make sure that the two columns lists have the same size
      val matchedColumns = localCols zip foreignCols
      ConstraintOp(true, DDLForeignKey(tn, kn, matchedColumns, ftn))
    }
  }

  def alterTableAdd = alterTableAddPrimary | alterTableAddForeign

  // TODO -- Add handling for droping foreign keys
  def alterTableDrop: Parser[CatalogOp] =
    "ALTER TABLE" ~> tableName <~ "DROP" ~ "PRIMARY" ~ "KEY" ^^ {
      case nm => ConstraintOp(false, DDLPrimaryKey(nm, null))
    }

  def alterTable = alterTableAdd | alterTableDrop

  def dropTable = """(?i)DROP TABLE""".r ~> tableName ^^ { DropTable(_) }

  def useSchema: Parser[CatalogOp] = "USE" ~> schemaName ^^ { UseSchema(_) }

  def statement = (useSchema | createTable | alterTable | dropTable) ~ statementTermination ^^ {
    case res ~ _ => res
  }

  def ddlDef = statement*

  def parse(sql: String) = parseAll(ddlDef, sql) map (_.toList) getOrElse (
    throw new Exception("Unable to parse DDL or constraints statement!"))
}