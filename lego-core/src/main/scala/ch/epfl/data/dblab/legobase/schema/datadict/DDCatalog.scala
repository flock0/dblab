package ch.epfl.data
package dblab.legobase
package schema.datadict

import sc.pardis.types.Tpe
import schema._

object DDCatalog extends Catalog {
  override def findSchema(name: String): Schema = ???
  override def getSchemaOrElseCreate(name: String): Schema = ???
  override def createAttribute(name: String, dataType: Tpe, constraints: Seq[Constraint]): Attribute = ???
}
case class DDSchema() extends Schema {
  override def findTable(name: String) = ???
  override def findAttribute(attrName: String): Option[Attribute] = ??? //TODO assumes that all attribute names are unique
  override def addTable(name: String, attributes: Seq[Attribute], fileName: String, rowCount: Long) = ???
  override def dropTable(tableName: String): Unit = ???
  override def toString = ???
}
case class DDTable() extends Table {
  override def primaryKey: Option[PrimaryKey] = ???
  override def dropPrimaryKey = ???
  override def foreignKeys: List[ForeignKey] = ???
  override def foreignKey(foreignKeyName: String): Option[ForeignKey] = ???
  override def dropForeignKey(foreignKeyName: String) = ???
  override def notNulls: List[NotNull] = ???
  override def uniques: List[Unique] = ???
  override def autoIncrement: Option[AutoIncrement] = ???
  override def findAttribute(attrName: String): Option[Attribute] = ???
  override def addConstraint(cstr: Constraint) = ???
  override def load: Array[_] = ???
}
case class DDAttribute() extends Attribute {
  override def hasConstraint(con: Constraint) = ???
  override def toString() = ???
}