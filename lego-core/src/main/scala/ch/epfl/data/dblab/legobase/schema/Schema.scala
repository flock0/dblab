package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions

case class Schema(tables: List[Table])
case class Table(name: String, attributes: List[Attribute], constraints: List[Constraint], fileName: String = "", var rowCount: Long = 0) {
  def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }
  def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }
  def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }
  def autoIncrement: Option[AutoIncrement] = constraints.collectFirst { case ainc: AutoIncrement => ainc }
}
case class Attribute(name: String, dataType: Tpe, var distinctValuesCount: Int = 0, var nullValuesCount: Long = 0)
object Attribute {
  implicit def tuple2ToAttribute(nameAndType: (String, Tpe)): Attribute = Attribute(nameAndType._1, nameAndType._2)
}

sealed trait Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int): DDConstraintsRecord
}
case class PrimaryKey(attributes: List[Attribute]) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) =
    DDConstraintsRecord(
      tableId,
      'p',
      catalog.getAttributes(tableId, attributes.map(_.name)).map(_.attributeId),
      None,
      None)
}
case class ForeignKey(ownTable: String, referencedTable: String, attributes: List[(String, String)], var selectivity: Double = 1) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) = {
    val fks: List[(Int, String)] = attributes map { case (loc: String, ref: String) => catalog.getAttribute(tableId, loc).attributeId -> ref }
    val (attributesList, refAttributesList) = fks.unzip

    DDConstraintsRecord(
      tableId,
      'f',
      attributesList,
      Some(referencedTable),
      Some(refAttributesList))
  }
  def foreignTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == referencedTable)
  def thisTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == ownTable)
  def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] = attributes.map { case (localAttr, foreignAttr) => thisTable.get.attributes.find(a => a.name == localAttr).get -> foreignTable.get.attributes.find(a => a.name == foreignAttr).get }
}
case class NotNull(attribute: Attribute) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) =
    DDConstraintsRecord(
      tableId,
      'n',
      List(catalog.getAttribute(tableId, attribute.name).attributeId),
      None,
      None)
}
case class Unique(attribute: Attribute) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) =
    DDConstraintsRecord(
      tableId,
      'u',
      List(catalog.getAttribute(tableId, attribute.name).attributeId),
      None,
      None)
}
case class AutoIncrement(attribute: Attribute) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) =
    throw new Exception("AutoIncrement doesn't have a representation as a DDConstraintsRecord. It is handled through DDSequenceRecords")
}
