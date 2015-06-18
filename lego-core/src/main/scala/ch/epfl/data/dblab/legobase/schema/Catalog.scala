package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types.Tpe
import scala.collection.mutable.ArrayBuffer

trait Catalog {
  def findSchema(name: String): Schema
  def getSchemaOrElseCreate(name: String): Schema
  def createAttribute(name: String, dataType: Tpe, constraints: List[Constraint] = List()): Attribute
}

trait Schema {
  def stats: Statistics
  def tables: List[Table]
  def addTable(name: String, attributes: List[Attribute], constraints: ArrayBuffer[Constraint], fileName: String, rowCount: Long)
  def dropTable(tableName: String)
  def findTable(name: String): Table
  def findAttribute(name: String): Option[Attribute]
  def toString: String
}

trait Table {
  def name: String
  def attributes: List[Attribute]
  def constraints: ArrayBuffer[Constraint]
  def findAttribute(name: String): Option[Attribute]
  def primaryKey: Option[PrimaryKey]
  def dropPrimaryKey
  def foreignKeys: List[ForeignKey]
  def foreignKey(foreignKeyName: String): Option[ForeignKey]
  def dropForeignKey(foreignKeyName: String)
  def addConstraint(cstr: Constraint)
  def notNulls: List[NotNull]
  def uniques: List[Unique]
  def autoIncrement: Option[AutoIncrement]
  def fileName: String
  def toString: String
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
case class PrimaryKey(attributes: List[Attribute]) extends Constraint {
  override def toString() = "PrimaryKey(" + attributes.map(a => a.name).mkString(",") + ")"
}
case class ForeignKey(foreignKeyName: String, ownTable: String, referencedTable: String, attributes: List[(String, String)]) extends Constraint {
  override def toString() = "ForeignKey(" + attributes.map(_._1).mkString(",") + ") references " + referencedTable + "(" + attributes.map(_._2).mkString(",") + ")"
  def foreignTable(implicit s: Schema): Table = s.findTable(referencedTable)
  def thisTable(implicit s: Schema): Table = s.findTable(ownTable)
  def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] = attributes.map { case (localAttr, foreignAttr) => thisTable.findAttribute(localAttr).get -> foreignTable.findAttribute(foreignAttr).get }
}
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint
object Compressed extends Constraint {
  override def toString = "COMPRESSED"
}