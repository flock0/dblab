package ch.epfl.data
package dblab.legobase
package schema.datadict

import sc.pardis.types.Tpe
import schema._

object DDCatalog extends Catalog {

  lazy val dict = DataDictionary()

  override def findSchema(schemaName: String): Schema = DDSchema(dict, name)
  override def getSchemaOrElseCreate(schemaName: String): Schema = findSchema(name)
  override def createAttribute(attrName: String, dataType: Tpe, constraints: Seq[Constraint]): Attribute = DDAttribute(dict, attrName, dataType, constraints)
}
case class DDSchema(private val dict: DataDictionary, name: String) extends Schema {
  override def stats: Statistics = dict.getStats(name)
  override def tables: Seq[Table] = dict.getTables(name).map(DDTable(dict, _))
  override def findTable(tableName: String) = dict.getTable(name, tableName).map(DDTable(dict, _))
  override def findAttribute(attrName: String): Option[Attribute] = { //TODO assumes that all attribute names are unique
    val filteredAttr = dict.getAttributes(attrName)
    if (filteredAttr.size != 1)
      throw new Exception(s"Attribute $attrName is not unique")
    filteredAttr(0)
  }
  override def addTable(tableName: String, attributes: Seq[Attribute], fileName: String, rowCount: Long) = ???
  override def dropTable(tableName: String): Unit = dict.dropTable(name, tableName)
  override def toString = {
    val schemaName = name
    val header = "\nSchema " + schemaName + "\n" + "-" * (schemaName.length + 8) + "\n\n"
    tables.foldLeft(header)((str, t) => {
      str + t.name + " (" + t.attributes.mkString("\n", "\n", "\n") + ");\n" + t.constraints.mkString("\n") + "\n\n"
    })
  }
}
case class DDTable(private val dict: DataDictionary, private val rec: TableRecord) extends Table {
  override def name: String = rec.name   
  override def fileName: String = rec.fileName match {
    case Some(fn) => fn
    case None => ""
  }
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
  override def attributes: Seq[Attribute] = ???
  override def constraints: Seq[Constraint] = ???
  override def load: Array[_] = ???
}
case class DDAttribute(private val dict: DataDictionary, name: String, dataType: Tpe, constraints: Seq[Constraint]) extends Attribute {
  override def hasConstraint(con: Constraint) = constraints contains con
  override def toString() = ???
}