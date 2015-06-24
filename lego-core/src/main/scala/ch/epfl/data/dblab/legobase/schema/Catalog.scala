package ch.epfl.data
package dblab.legobase
package schema

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