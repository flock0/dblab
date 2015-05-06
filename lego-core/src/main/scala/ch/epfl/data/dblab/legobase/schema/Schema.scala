package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions

case class Catalog(schemata: Map[String, Schema]) {
    case class DDTablesRecord(schemaName: String, tableId: Int, name: String)
    case class DDAttributesRecord(schemaName: String, tableId: Int, attributeId: Int, name: String, dataType: Tpe)
    case class DDRowsRecord(schemaName: String, tableId: Int, rowId: Int)
    case class DDFieldsRecord(schemaName: String, tableId: Int, attributeId: Int, rowId: Int, value: FieldValue)
    val DataDictionarySchemaName = "DD"

    val dataDictionary: List[Table] = {
        val tablesTable = {
          val schemaName: Attribute = "SCHEMA_NAME" -> StringType
          val tableId: Attribute = "TABLE_ID" -> IntType

          new Table("DD_TABLES", List(
            schemaName,
            tableId,
            "NAME" -> StringType
            List(PrimaryKey(List(schemaName, tableId))))
        }
        val attributesTable = {
          val schemaName: Attribute = "SCHEMA_NAME" -> StringType
          val tableId: Attribute = "TABLE_ID" -> IntType
          val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType
          
          new Table("DD_ATTRIBUTES", List(
            schemaName,
            tableId,
            attributeId,
            "NAME" -> StringType,
            "DATATYPE" -> Tpe
            List(PrimaryKey(List(schemaName, tableId, attributeId)),
                ForeignKey("DD_ATTRIBUTES", "DD_TABLES", List(("SCHEMA_NAME", "SCHEMA_NAME"), ("TABLE_ID", "TABLE_ID"))))))
        }
        val rowsTable = {
          val schemaName: Attribute = "SCHEMA_NAME" -> StringType
          val tableId: Attribute = "TABLE_ID" -> IntType
          val rowId: Attribute = "ROW_ID" -> IntType
          
          new Table("DD_ROWS", List(
            schemaName,
            tableId,
            rowId
            List(PrimaryKey(List(schemaName, tableId, rowId)),
                ForeignKey("DD_ROWS", "DD_TABLES", List(("SCHEMA_NAME", "SCHEMA_NAME"), ("TABLE_ID", "TABLE_ID"))))))
        }
        val fieldsTable = {
          val schemaName: Attribute = "SCHEMA_NAME" -> StringType
          val tableId: Attribute = "TABLE_ID" -> IntType
          val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType
          val rowId: Attribute = "ROW_ID" -> IntType
          
          new Table("DD_FIELDS", List(
            schemaName,
            tableId,
            attributeId,
            rowId,
            "VALUE" -> 
            List(PrimaryKey(List(schemaName, tableId, attributeId, rowId)),
                ForeignKey("DD_FIELDS", "DD_ATTRIBUTES", List(("SCHEMA_NAME", "SCHEMA_NAME"), ("TABLE_ID", "TABLE_ID"), ("ATTRIBUTE_ID", "ATTRIBUTE_ID"))),
                ForeignKey("DD_FIELDS", "DD_ROWS", List(("SCHEMA_NAME", "SCHEMA_NAME"), ("TABLE_ID", "TABLE_ID"), ("ROW_ID", "ROW_ID"))))))
        }

        List(tablesTable, attributesTable, rowsTable, fieldsTable)
    }

    var ddTables: Set[DDTablesRecord] = Set.empty
    var ddAttributes: Set[DDAttributesRecord] = Set.empty
    var ddRows: Set[DDAttributesRecord] = Set.empty
    var ddFields: Set[DDFieldsRecord] = Set.empty

    private def addTableToDD(schemaName: String, tbl: Table)) = {
        val newTableId = Random.nextInt //TODO Get tableID from sequences, not Random
        ddTables += DDTablesRecord(schemaName, newTableId, tbl.name)
        tbl.attributes.foreach { attr =>
            ddAttributes += DDAttributesRecord(schemaName, newTableId, Random.nextInt, attr.name, attr.dataType) //TODO Get attributeID from sequences, not Random
        }
        
        //TODO add entries to rows
    }
    //TODO Populate DD with schemata
    //TODO Implement Unique constraint and AutoIncrement constraint / Sequences
}

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

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint
case class ForeignKey(ownTable: String, referencedTable: String, attributes: List[(String, String)], var selectivity: Double = 1) extends Constraint {
  def foreignTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == referencedTable)
  def thisTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == ownTable)
  def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] = attributes.map { case (localAttr, foreignAttr) => thisTable.get.attributes.find(a => a.name == localAttr).get -> foreignTable.get.attributes.find(a => a.name == foreignAttr).get }
}
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint
