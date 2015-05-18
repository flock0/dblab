package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions

case class Schema(tables: List[Table])
case class Table(name: String, attributes: List[Attribute], constraints: List[Constraint], resourceLocator: String = "", var rowCount: Long = 0) {
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
      catalog.ddAttributes.filter(at => at.tableId == tableId && attributes.map(_.name).contains(at.name)).map(_.attributeId),
      None,
      None)
}
case class ForeignKey(ownTable: String, referencedTable: String, attributes: List[(String, String)], var selectivity: Double = 1) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) = {
    def findAttributeId(tID: Int, attributeName: String) =
      catalog.ddAttributes.find(at => at.tableId == tID && attributeName == at.name) match {
        case Some(a) => a.attributeId
        case None    => throw new Exception(s"Couldn't find attribute $attributeName in table $tID")
      }

    val fks: List[(Int, String)] = attributes map { case (loc: String, ref: String) => findAttributeId(tableId, loc) -> ref }
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
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) = {
    val attributeId = catalog.ddAttributes.find(at => at.tableId == tableId && attribute.name == at.name) match {
      case Some(a) => a.attributeId
      case None    => throw new Exception(s"Couldn't find attribute ${attribute.name}")
    }

    DDConstraintsRecord(
      tableId,
      'n',
      List(attributeId),
      None,
      None)
  }
}
case class Unique(attribute: Attribute) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) = {
    val attributeId = catalog.ddAttributes.find(at => at.tableId == tableId && attribute.name == at.name) match {
      case Some(a) => a.attributeId
      case None    => throw new Exception(s"Couldn't find attribute ${attribute.name}")
    }

    DDConstraintsRecord(
      tableId,
      'u',
      List(attributeId),
      None,
      None)
  }
}
case class AutoIncrement(attribute: Attribute) extends Constraint {
  def toDDConstraintsRecord(catalog: Catalog, tableId: Int) = ??? //TODO AutoIncrement doesn't really belong to the Constraint trait
}
