package ch.epfl.data
package dblab.legobase
package frontend

import schema._
import DDLParser._
import utils._
import sc.pardis.types._
import scala.collection.immutable.Set
import scala.collection.mutable.ArrayBuffer

object DDLInterpreter {
  var currSchema: Option[Schema] = None

  // TODO -- This can go away (and thus DDLTypes becoming SCTypes) 
  // if SC types become generic enough to handle precision of SQL
  // e.g. in doubles and char types
  def ddlTypeToSCType(ddlType: AttributeType) = {
    ddlType match {
      case c: DDLInteger => IntType
      case c: DDLDecimal => DoubleType
      case c: DDLVarChar => VarCharType(c.numChars)
      case c: DDLChar    => if (c.numChars == 1) CharType else VarCharType(c.numChars) // TODO -- Make SC Char take numChars as arg
      case c: DDLDate    => DateType
    }
  }

  def annonStringToConstraint(consStr: String) = consStr match {
    case "COMPRESSED" => Compressed
  }

  def getCurrSchema = {
    currSchema match {
      case None =>
        throw new Exception("Current schema not defined. Have you used USE <SCHEMA_NAME> to specify your schema?")
      case Some(schema) => schema
    }
  }

  def interpret(ddlDef: List[_]): Schema = {
    ddlDef.foreach(ddl => ddl match {
      case UseSchema(schemaName) => {
        currSchema = Some(Catalog.schemata.getOrElseUpdate(schemaName, new Schema()))
      }
      case DropTable(tableName) => {
        val schema = getCurrSchema
        schema.tables -= schema.findTable(tableName)
      }
      case DDLTable(tableName, cols, cons) =>
        val colDef = cols.map(c => Attribute(c.name, ddlTypeToSCType(c.datatype), c.annotations.map(annonStringToConstraint(_)))).toList
        val tablePath = (Config.datapath + tableName.toLowerCase + ".tbl")
        val tableCardinality = Utilities.getNumLinesInFile(tablePath)
        getCurrSchema.tables += new Table(tableName, colDef, ArrayBuffer(),
          tablePath, tableCardinality)
        interpret(cons.toList)
        // Update cardinality stat for this schema
        getCurrSchema.stats += "CARDINALITY_" + tableName -> tableCardinality
        getCurrSchema
      case ConstraintOp(add, cons) => add match {
        case true => /* add */
          cons match {
            case pk: DDLPrimaryKey =>
              val table = getCurrSchema.findTable(pk.table)
              val primaryKeyAttrs = pk.primaryKeyCols.map(pkc => table.findAttribute(pkc).get)
              table.constraints += PrimaryKey(primaryKeyAttrs)
            case fk: DDLForeignKey =>
              val table = getCurrSchema.findTable(fk.table)
              val foreignKeyAttrs = fk.foreignKeyCols.map(fkc => table.findAttribute(fkc).get)
              table.constraints += ForeignKey(fk.foreignKeyName, fk.table, fk.foreignTable, foreignKeyAttrs)
          }
        case false => /* drop */
          cons match {
            case pk: DDLPrimaryKey =>
              getCurrSchema.findTable(pk.table).dropPrimaryKey
            case fk: DDLForeignKey =>
              getCurrSchema.findTable(fk.table).dropForeignKey(fk.foreignKeyName)
          }
      }
    })
    getCurrSchema
  }
}