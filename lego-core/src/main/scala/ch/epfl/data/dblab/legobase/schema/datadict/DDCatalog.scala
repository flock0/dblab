package ch.epfl.data
package dblab.legobase
package schema.datadict

import sc.pardis.types.{ Tpe, PardisType }
import schema._

object DDCatalog extends Catalog {

  lazy val dict = DataDictionary()

  override def findSchema(schemaName: String): Schema = DDSchema(dict, schemaName)
  override def getSchemaOrElseCreate(schemaName: String): Schema = findSchema(schemaName)
}
case class DDSchema(private val dict: DataDictionary, name: String) extends Schema {
  override def stats: Statistics = dict.getStats(name)
  override def tables: Seq[Table] = dict.getTables(name).map(DDTable(dict, _))
  override def findTable(tableName: String) = DDTable(dict, dict.getTable(name, tableName))
  override def findAttribute(attrName: String): Option[Attribute] = { //TODO assumes that all attribute names are unique
    val filteredAttr = dict.getAttributes(attrName)
    if (filteredAttr.size == 0)
      None
    else if (filteredAttr.size != 1)
      throw new Exception(s"Attribute $attrName is not unique")
    else
      Some(DDAttribute(dict, filteredAttr(0)))
  }
  override def addTable(tableName: String, attributes: Seq[(String, PardisType[_], Seq[Constraint])], fileName: String, rowCount: Long) =
    dict.addTable(name, tableName, attributes, fileName)
  override def dropTable(tableName: String): Unit = dict.dropTable(name, tableName)
  override def toString = {
    val schemaName = name
    val header = "\nSchema " + schemaName + "\n" + "-" * (schemaName.length + 8) + "\n\n"
    tables.foldLeft(header)((str, t) => {
      str + t.name + " (" + t.attributes.mkString("\n", "\n", "\n") + ");\n" + t.constraints.mkString("\n") + "\n\n"
    })
  }
}
case class DDTable(private val dict: DataDictionary, private val rec: TablesRecord) extends Table {
  implicit val d = dict
  override def name: String = rec.name
  override def fileName: String = rec.fileName match {
    case Some(fn) => fn
    case None     => ""
  }
  override def primaryKey: Option[PrimaryKey] = {
    val pks: List[PrimaryKey] = dict.getConstraints(rec.tableId, 'p').toList
    if (pks.size == 0)
      None
    else if (pks.size == 1)
      Some(pks.head)
    else
      throw new Exception(s"Expected exactly 1 PrimaryKey constraint but found ${pks.size}.")
  }
  override def dropPrimaryKey = dict.dropPrimaryKey(rec.tableId)
  override def foreignKeys: List[ForeignKey] = dict.getConstraints(rec.tableId, 'f').toList
  override def foreignKey(foreignKeyName: String): Option[ForeignKey] = foreignKeys.find(_.foreignKeyName == foreignKeyName)
  override def dropForeignKey(foreignKeyName: String) = dict.dropForeignKey(rec.tableId, foreignKeyName)
  override def notNulls: List[NotNull] = dict.getConstraints(rec.tableId, 'n').toList
  override def uniques: List[Unique] = dict.getConstraints(rec.tableId, 'u').toList
  override def autoIncrement: Option[AutoIncrement] = ??? //TODO AutoIncrement is represented as a sequence in the data dictionary
  override def findAttribute(attrName: String): Option[Attribute] = Some(DDAttribute(dict, dict.getAttribute(rec.tableId, attrName))) //TODO None case is handeled through exception
  override def addConstraint(cstr: Constraint) = dict.addConstraint(cstr)
  override def attributes: Seq[Attribute] = dict.getAttributes(rec.tableId).map(DDAttribute(dict, _))
  override def constraints: Seq[Constraint] = dict.getConstraints(rec.tableId).toList.map(Constraint.ConstraintsRecordToConstraint(_))
  override def load: Array[_ <: Record] = dict.getTuples(rec)
}
case class DDAttribute(private val dict: DataDictionary, private val attr: AttributesRecord) extends Attribute {
  override def name: String = attr.name
  override def dataType: Tpe = attr.dataType
  def constraints: Seq[Constraint] = dict.getConstraints(attr.name)
  override def hasConstraint(con: Constraint) = constraints contains con
  override def toString =
    "    " + "%-20s".format(name) + "%-20s".format(dataType) + constraints.map(c => "@%-10s".format(c)).mkString(" , ")
}