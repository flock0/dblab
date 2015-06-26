package ch.epfl.data
package dblab.legobase
package schema

import scala.language.implicitConversions
import sc.pardis.types.{ Tpe, PardisType }
import scala.collection.mutable.ArrayBuffer

trait Catalog {
  def findSchema(schemaName: String): Schema
  def getSchemaOrElseCreate(schemaName: String): Schema
}

trait Schema {
  def stats: Statistics
  def tables: Seq[Table]
  def addTable(tableName: String, attributes: Seq[(String, PardisType[_], Seq[Constraint])], fileName: String, rowCount: Long)
  def dropTable(tableName: String)
  def findTable(tableName: String): Table
  def findAttribute(attrName: String): Option[Attribute]
  def toString: String
}

trait Table {
  def name: String
  def attributes: Seq[Attribute]
  def constraints: Seq[Constraint]
  def findAttribute(attrName: String): Option[Attribute]
  def primaryKey: Option[PrimaryKey]
  def dropPrimaryKey
  def foreignKeys: Seq[ForeignKey]
  def foreignKey(foreignKeyName: String): Option[ForeignKey]
  def dropForeignKey(foreignKeyName: String)
  def addConstraint(cstr: Constraint)
  def notNulls: Seq[NotNull]
  def uniques: Seq[Unique]
  def autoIncrement: Option[AutoIncrement]
  def fileName: String
  def toString: String
  def load: Array[_ <: Record]
}

trait Attribute {
  def name: String
  def dataType: Tpe
  def hasConstraint(con: Constraint)
  def toString: String
}

sealed trait Constraint {
  def toString: String
}
case class PrimaryKey(attributes: Seq[Attribute]) extends Constraint {
  override def toString = "PrimaryKey(" + attributes.map(a => a.name).mkString(",") + ")"
}
case class ForeignKey(foreignKeyName: String, ownTable: String, referencedTable: String, attributes: Seq[(String, String)]) extends Constraint {
  override def toString = "ForeignKey(" + attributes.map(_._1).mkString(",") + ") references " + referencedTable + "(" + attributes.map(_._2).mkString(",") + ")"
  def foreignTable(implicit s: Schema): Table = s.findTable(referencedTable)
  def thisTable(implicit s: Schema): Table = s.findTable(ownTable)
  def matchingAttributes(implicit s: Schema): Seq[(Attribute, Attribute)] = attributes.map { case (localAttr, foreignAttr) => thisTable.findAttribute(localAttr).get -> foreignTable.findAttribute(foreignAttr).get }
}
case class NotNull(attribute: Attribute) extends Constraint {
  override def toString = s"NotNull($attribute)"
}
case class Unique(attribute: Attribute) extends Constraint {
  override def toString = s"Unique($attribute)"
}
case class AutoIncrement(attribute: Attribute) extends Constraint {
  override def toString = s"Unique($attribute)"
}
object Compressed extends Constraint {
  override def toString = "COMPRESSED"
}

object Constraint {
  import datadict.{ ConstraintsRecord, DataDictionary, DDAttribute }
  def ConstraintsRecordToConstraint(cr: ConstraintsRecord)(implicit dict: DataDictionary): Constraint = {
    cr.constraintType match {
      case 'p' => PrimaryKey(dict.getAttributesFromIds(cr.tableId, cr.attributes).map(a => DDAttribute(dict, a)))
      case 'f' => {
        val referencedTableName = cr.refTableName match {
          case Some(name) => name
          case None       => throw new Exception(s"ForeignKey ${cr.foreignKeyName} is missing the name of the referenced table.")
        }
        val referencedAttributes = cr.refAttributes match {
          case None            => throw new Exception(s"ForeignKey ${cr.foreignKeyName} is missing the attributes it refers to.")
          case Some(attrNames) => attrNames
        }
        val foreignKeyName = cr.foreignKeyName match {
          case None       => throw new Exception(s"ForeignKey ${cr.foreignKeyName} is missing a name.")
          case Some(name) => name
        }
        val ownTable = dict.getTable(cr.tableId).name
        val columnAssignments = dict.getAttributesFromIds(cr.tableId, cr.attributes).map(at => at.name).zip(referencedAttributes)
        ForeignKey(foreignKeyName, ownTable, referencedTableName, columnAssignments)
      }
      case 'n' => NotNull(DDAttribute(dict, dict.getAttribute(cr.tableId, cr.attributes.head)))
      case 'u' => Unique(DDAttribute(dict, dict.getAttribute(cr.tableId, cr.attributes.head)))
      case 'c' => Compressed //(DDAttribute(dict, dict.getAttribute(cr.tableId, cr.attributes.head)))
      case _   => throw new Exception(s"Constraint in ${cr.tableId} has unknown type '${cr.constraintType}'.")
    }
  }
  implicit def ConstraintsRecordToPrimaryKey(cr: ConstraintsRecord)(implicit dict: DataDictionary): PrimaryKey =
    ConstraintsRecordToConstraint(cr) match {
      case x: PrimaryKey => x
      case x             => throw new Exception(s"Expected to obtain a PrimaryKey but found a ${x.getClass.getName}.")
    }
  implicit def ConstraintsRecordToForeignKey(cr: ConstraintsRecord)(implicit dict: DataDictionary): ForeignKey =
    ConstraintsRecordToConstraint(cr) match {
      case x: ForeignKey => x
      case x             => throw new Exception(s"Expected to obtain a ForeignKey but found a ${x.getClass.getName}.")
    }
  implicit def ConstraintsRecordToNotNull(cr: ConstraintsRecord)(implicit dict: DataDictionary): NotNull =
    ConstraintsRecordToConstraint(cr) match {
      case x: NotNull => x
      case x          => throw new Exception(s"Expected to obtain a NotNull but found a ${x.getClass.getName}.")
    }
  implicit def ConstraintsRecordToUnique(cr: ConstraintsRecord)(implicit dict: DataDictionary): Unique =
    ConstraintsRecordToConstraint(cr) match {
      case x: Unique => x
      case x         => throw new Exception(s"Expected to obtain a Unique but found a ${x.getClass.getName}.")
    }
  def ConstraintsRecordListToConstraintList(lst: List[ConstraintsRecord])(implicit dict: DataDictionary): List[Constraint] = lst.map(ConstraintsRecordToConstraint(_))
  implicit def ConstraintsRecordListToPrimaryKeyList(lst: List[ConstraintsRecord])(implicit dict: DataDictionary): List[PrimaryKey] = lst.map(ConstraintsRecordToPrimaryKey(_))
  implicit def ConstraintsRecordListToForeignKeyList(lst: List[ConstraintsRecord])(implicit dict: DataDictionary): List[ForeignKey] = lst.map(ConstraintsRecordToForeignKey(_))
  implicit def ConstraintsRecordListToNotNullList(lst: List[ConstraintsRecord])(implicit dict: DataDictionary): List[NotNull] = lst.map(ConstraintsRecordToNotNull(_))
  implicit def ConstraintsRecordListToUniqueList(lst: List[ConstraintsRecord])(implicit dict: DataDictionary): List[Unique] = lst.map(ConstraintsRecordToUnique(_))
}