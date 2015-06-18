package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions
import scala.collection.mutable.ListMap
import scala.collection.mutable.ArrayBuffer

object MapCatalog extends Catalog {
  val schemata = new scala.collection.mutable.HashMap[String, MapSchema]()
}
case class MapSchema(tables: ArrayBuffer[Table] = ArrayBuffer(), stats: Statistics = Statistics()) extends Schema {
  def findTable(name: String) = tables.find(t => t.name == name) match {
    case Some(tab) => tab
    case None      => throw new Exception("Table " + name + " not found in schema!")
  }
  def findAttribute(attrName: String): Option[Attribute] = tables.map(t => t.findAttribute(attrName)).flatten.toList match {
    case List() => None //throw new Exception("Attribute " + attrName + " not found in schema!")
    case l      => Some(l.apply(0)) // todo -- OK, but assumes that all attribute names are unique
  }
  override def toString() = {
    val schemaName = MapCatalog.schemata.find({ case (k, v) => v.equals(this) }).get._1
    val header = "\nSchema " + schemaName + "\n" + "-" * (schemaName.length + 8) + "\n\n"
    tables.foldLeft(header)((str, t) => {
      str + t.name + " (" + t.attributes.mkString("\n", "\n", "\n") + ");\n" + t.constraints.mkString("\n") + "\n\n"
    })
  }
}
case class MapTable(name: String, attributes: List[Attribute], constraints: ArrayBuffer[Constraint], resourceLocator: String, var rowCount: Long) extends Table {
  def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  def dropPrimaryKey = primaryKey match {
    case Some(pk) => constraints -= pk
    case None     => System.out.println(s"${scala.Console.YELLOW}Warning${scala.Console.RESET}: Primary Key for table " + name + " cannot be dropped as it does not exist!")
  }
  def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }.toList
  def foreignKey(foreignKeyName: String): Option[ForeignKey] = foreignKeys.find(f => f.foreignKeyName == foreignKeyName)
  def dropForeignKey(foreignKeyName: String) = foreignKey(foreignKeyName) match {
    case Some(fk) => constraints -= fk
    case None     => System.out.println(s"${scala.Console.YELLOW}Warning${scala.Console.RESET}: ForeignKey Key " + foreignKeyName + " for table " + name + " cannot be dropped as it does not exist!")
  }
  def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }.toList
  def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }.toList
  def autoIncrement: Option[AutoIncrement] = constraints.collectFirst { case ainc: AutoIncrement => ainc }
  def findAttribute(attrName: String): Option[Attribute] = attributes.find(attr => attr.name == attrName)
}
case class MapAttribute(name: String, dataType: Tpe, constraints: List[Constraint] = List()) extends Attribute {
  def hasConstraint(con: Constraint) = constraints.contains(con)
  override def toString() = {
    "    " + "%-20s".format(name) + "%-20s".format(dataType) + constraints.map(c => "@%-10s".format(c)).mkString(" , ")
  }
}