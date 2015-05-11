package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions

case class Catalog(schemata: Map[String, Schema]) {
    
    /* Case classes for the tables in the data dictionary */
    case class DDTablesRecord(schemaName: String, name: String, tableId: Int = getSequence(DataDictionarySchemaName + "DD_TABLES_TABLE_ID_SEQ"))
    case class DDAttributesRecord(tableId: Int, name: String, dataType: Tpe, attributeId: Int = getSequence(DataDictionarySchemaName + "DD_ATTRIBUTES_ATTRIBUTE_ID_SEQ"))
    case class DDFieldsRecord(tableId: Int, attributeId: Int, rowId: Int, value: Any)
    case class DDRowsRecord(tableId: Int, rowId: Int = getSequence(DataDictionarySchemaName + "DD_ROWS_ROW_ID_SEQ"))
    case class DDConstraintsRecord(tableId: Int, constraintType: Char, attributes: List[Int], refTableID: Int, refAttributes: List[Int])
    case class DDSequencesRecord(startValue: Int, endValue: Int, : incrementBy: Int, sequenceName: String, sequenceId: Int = getSequence(DataDictionarySchemaName + "DD_SEQUENCES_SEQUENCE_ID_SEQ")) {

      //TODO Catch invalid start/end/increment combination

      private var next = startValue

      /**
       * Returns the next value of the sequence
       * 
       * @return The next value of the sequence
       * @throws Exception when the sequence has been exhausted
       */
      private def nextVal: Int = {
        if (next > maxValue)
          throw new Exception (s"Sequence $sequenceId has reached it's maximum value $maxValue")
        val value = next
        next += incrementBy
        value
      }
    }
    
    /** The default name of the schema for the data dictionary itself */
    val DataDictionarySchemaName = "DD"

    /** The tables that comprise the data dictionary */
    private val dataDictionary: List[Table] = {
        val tablesTable = {
          val schemaName: Attribute = "SCHEMA_NAME" -> StringType
          val tableId: Attribute = "TABLE_ID" -> IntType

          new Table("DD_TABLES", List(
            schemaName,
            "NAME" -> StringType,
            tableId,
            List(PrimaryKey(List(schemaName, tableId))))
        }
        val attributesTable = {
          val tableId: Attribute = "TABLE_ID" -> IntType
          val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType
          
          new Table("DD_ATTRIBUTES", List(
            tableId,
            "NAME" -> StringType,
            "DATATYPE" -> Tpe,
            attributeId,
            List(PrimaryKey(List(tableId, attributeId)),
                ForeignKey("DD_ATTRIBUTES", "DD_TABLES", List(("TABLE_ID", "TABLE_ID"))))))
        }
        val rowsTable = {
          val tableId: Attribute = "TABLE_ID" -> IntType
          val rowId: Attribute = "ROW_ID" -> IntType
          
          new Table("DD_ROWS", List(
            tableId,
            rowId
            List(PrimaryKey(List(schemaName, tableId, rowId)),
                ForeignKey("DD_ROWS", "DD_TABLES", List(("TABLE_ID", "TABLE_ID"))))))
        }
        val fieldsTable = {
          val tableId: Attribute = "TABLE_ID" -> IntType
          val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType
          val rowId: Attribute = "ROW_ID" -> IntType
          
          new Table("DD_FIELDS", List(
            tableId,
            attributeId,
            rowId,
            "VALUE" -> AnyType
            List(PrimaryKey(List(tableId, attributeId, rowId)),
                ForeignKey("DD_FIELDS", "DD_ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTE_ID", "ATTRIBUTE_ID"))),
                ForeignKey("DD_FIELDS", "DD_ROWS", List(("TABLE_ID", "TABLE_ID"), ("ROW_ID", "ROW_ID"))))))
        }
        val constraintsTable = {
          val tableId: Attribute = "TABLE_ID" -> IntType
          
          new Table("DD_CONSTRAINTS", List(
            tableId,
            "CONSTRAINT_TYPE" -> CharType /* f = foreign key constraint, p = primary key constraint, u = unique constraint, n = notnull constraint */
            "ATTRIBUTES" -> SeqType[IntType],
            "REF_TABLE_ID" -> Option[IntType],
            "REF_ATTRIBUTES" -> Option[SeqType[IntType]],
            List(ForeignKey("DD_CONSTRAINTS", "DD_ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTES", "ATTRIBUTE_ID"))),
                ForeignKey("DD_CONSTRAINTS", "DD_ATTRIBUTES", List(("REF_TABLE_ID", "TABLE_ID"), ("REF_ATTRIBUTES", "ATTRIBUTE_ID"))))))
        }
        val sequencesTable = {
          val sequenceId: Attribute = "SEQUENCE_ID" -> IntType
          
          new Table("DD_SEQUENCES", List(
            "START_VALUE" -> IntType,
            "END_VALUE" -> IntType,
            "INCREMENT_BY" -> IntType,
            "NAME" -> StringType,
            sequenceId,
            List(PrimaryKey(sequenceId), 
                Unique("NAME"))))
        }

        List(tablesTable, attributesTable, rowsTable, fieldsTable, constraintsTable, sequencesTable)
    }

    /* Lists that contain the data in the data dictionary*/
    var ddTables: List[DDTablesRecord] = List.empty
    var ddAttributes: List[DDAttributesRecord] = List.empty
    var ddRows: List[DDAttributesRecord] = List.empty
    var ddFields: List[DDFieldsRecord] = List.empty
    var ddConstraints: List[DDConstraintsRecord] = List.empty
    var ddSequences: List[DDSequencesRecord] = List.empty

    /** Initializes the data dictionary with itself */
    private def initializeDD() = {

      /* Create initial sequences for the DD relations */
      ddSequences += DDSequencesRecord(1, Int.MaxValue, 1, DataDictionarySchemaName + "DD_SEQUENCES_SEQUENCE_ID_SEQ", 0)
      ddSequences += DDSequencesRecord(0, Int.MaxValue, 1, DataDictionarySchemaName + "DD_TABLES_TABLE_ID_SEQ")
      ddSequences += DDSequencesRecord(0, Int.MaxValue, 1, DataDictionarySchemaName + "DD_ATTRIBUTES_ATTRIBUTE_ID_SEQ")
      ddSequences += DDSequencesRecord(0, Int.MaxValue, 1, DataDictionarySchemaName + "DD_ROWS_ROW_ID_SEQ")

      /* Fill DD_TABLES */
      (0 until dataDictionary.size) foreach { i =>
        val tbl = dataDictionary(i)
        ddTables += DDTablesRecord(DataDictionarySchemaName, tbl.name)
      }
      
      /* Fill DD_ROWS with tables and DD_ATTRIBUTES for each table */
      (0 until dataDictionary.size) foreach { i =>
        val tbl = dataDictionary(i)
        ddRows += DDRowsRecord(i)

        (0 until tbl.attributes.size) foreach { j =>
          ddAttributes += DDAttributesRecord(i, attr.name, attr.dataType)
          ddRows += DDRowsRecord(1)
        }
      }
      
      //TODO Fill ddRows with data from all the DD relations
      //TODO Fill ddFields with data from all the DD relations
      //TODO Fill ddConstraints
      //TODO Fill ddSequences
    }

    /** Adds a table to the given schema */
    private def addTableToDD(schemaName: String, tbl: Table) = {
        if (tableExistsAlready(schemaName, tbl)) {
          throw new Exception("Table " + tbl.name + " already exists in schema" + schemaName + ".")
        }

        /* Add entry to DD_TABLES*/
        val newTableId = getSequence(DataDictionarySchemaName + "DD_TABLES_TABLE_ID_SEQ").nextVal
        ddTables += DDTablesRecord(schemaName, tbl.name, newTableId)
        val newRow = DDRowsRecord(0)
        ddRows += newRow
        ddFields ++= for ( (att, val) <- getAttributes(DataDictionarySchemaName, "DD_TABLES").zip(Seq(schemaName, tbl.name, newTableId))) 
                      yield DDFieldsRecord(newTableId, att.attributeId, newRow.rowId, val)

        /* Add entries to DD_ATTRIBUTES */
        val newAttributes = for (attr <- tbl.attributes) yield DDAttributesRecord(newTableId, attr.name, attr.dataType)
        ddAttributes ++= newAttributes
        //TODO Add rows for attributes
        //TODO Add values for attributes
        val newConstraints = for (cst <- tbl.constraints) yield cst.toDDRecord
        ddConstraints ++= newConstraints
        //TODO Add rows for constraints
        //TODO Add values for constraints

        //TODO Add sequences
        //TODO Add rows for sequences
        //TODO Add values for sequences
    }

    /* Populate DD with schemata that have been passed to the constructor */
    schemata.foreach{  
      case (name, Schema(tables)) => tables.foreach { t =>
        addTableToDD(name, t)
      }
    }
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
