package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions
import Catalog._

/* Case classes for the tables in the data dictionary */
case class DDTablesRecord(schemaName: String, name: String, catalog: Catalog, private val _tableId: Option[Int] = None) {
  val tableId = _tableId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DataDictionarySchemaName, "DD_TABLES", "TABLE_ID"))
  }
}
case class DDAttributesRecord(tableId: Int, name: String, dataType: Tpe, catalog: Catalog, _attributeId: Option[Int] = None) {
  val attributeId = _attributeId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DataDictionarySchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID"))
  }
}
case class DDFieldsRecord(tableId: Int, attributeId: Int, rowId: Int, value: Any)
case class DDRowsRecord(tableId: Int, catalog: Catalog, _rowId: Option[Int] = None) {
  val rowId = _rowId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DataDictionarySchemaName, "DD_ROWS", "ROW_ID"))
  }
}
case class DDConstraintsRecord(tableId: Int, constraintType: Char, attributes: List[Int], refTableID: Int, refAttributes: List[Int])
case class DDSequencesRecord(startValue: Int, endValue: Int, incrementBy: Int, sequenceName: String, catalog: Catalog, _sequenceId: Option[Int] = None) {
  val sequenceId = _sequenceId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DataDictionarySchemaName, "DD_SEQUENCES", "SEQUENCE_ID"))
  }
  /* Catch invalid start/end/incrementBy values*/
  if (incrementBy == 0)
    throw new Exception("incrementBy must not be 0")
  if (startValue < endValue) {
    if (incrementBy < 0) {
      throw new Exception("Sequence can never reach the end value")
    }
  } else {
    if (incrementBy > 0) {
      throw new Exception("Sequence can never reach the end value")
    }
  }

  private var next = startValue

  /**
   * Returns the next value of the sequence
   *
   * @return The next value of the sequence
   * @throws Exception when the sequence has been exhausted
   */
  private def nextVal: Int = {
    if (next > endValue)
      throw new Exception(s"Sequence $sequenceId has reached it's maximum value $endValue")
    val value = next
    next += incrementBy
    //TODO Update field value in DD_FIELD for this sequence
    value
  }
}

object Catalog {
  /** Returns the standardized sequence name */
  def constructSequenceName(schemaName: String, tableName: String, attributeName: String) =
    schemaName + "_" + tableName + "_" + attributeName + "_SEQ"

  /** The default name of the schema for the data dictionary itself */
  val DataDictionarySchemaName = "DD"

  /** The tables that comprise the data dictionary */
  private val dataDictionary: List[Table] = {
    val tablesTable = {
      val schemaName: Attribute = "SCHEMA_NAME" -> StringType
      val name: Attribute = "NAME" -> StringType
      val tableId: Attribute = "TABLE_ID" -> IntType

      new Table("DD_TABLES", List(
        schemaName,
        name,
        tableId),
        List(PrimaryKey(List(schemaName, tableId)),
          NotNull(schemaName),
          NotNull(name),
          AutoIncrement(tableId)))
    }
    val attributesTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val name: Attribute = "NAME" -> StringType
      val datatype: Attribute = "DATATYPE" -> TpeType
      val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType

      new Table("DD_ATTRIBUTES", List(
        tableId,
        name,
        datatype,
        attributeId),
        List(PrimaryKey(List(tableId, attributeId)),
          ForeignKey("DD_ATTRIBUTES", "DD_TABLES", List(("TABLE_ID", "TABLE_ID"))),
          NotNull(name),
          NotNull(datatype),
          AutoIncrement(attributeId)))
    }
    val rowsTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val rowId: Attribute = "ROW_ID" -> IntType

      new Table("DD_ROWS", List(
        tableId,
        rowId),
        List(PrimaryKey(List(tableId, rowId)),
          ForeignKey("DD_ROWS", "DD_TABLES", List(("TABLE_ID", "TABLE_ID"))),
          AutoIncrement(rowId)))
    }
    val fieldsTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType
      val rowId: Attribute = "ROW_ID" -> IntType
      val value: Attribute = "VALUE" -> AnyType

      new Table("DD_FIELDS", List(
        tableId,
        attributeId,
        rowId,
        value),
        List(PrimaryKey(List(tableId, attributeId, rowId)),
          ForeignKey("DD_FIELDS", "DD_ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTE_ID", "ATTRIBUTE_ID"))),
          ForeignKey("DD_FIELDS", "DD_ROWS", List(("TABLE_ID", "TABLE_ID"), ("ROW_ID", "ROW_ID"))),
          NotNull(value)))
    }
    val constraintsTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val constraintType: Attribute = "CONSTRAINT_TYPE" -> CharType /* f = foreign key constraint, p = primary key constraint, u = unique constraint, n = notnull constraint */
      val attributes: Attribute = "ATTRIBUTES" -> SeqType(IntType)

      new Table("DD_CONSTRAINTS", List(
        tableId,
        constraintType,
        attributes,
        "REF_TABLE_ID" -> OptionType(IntType),
        "REF_ATTRIBUTES" -> OptionType(SeqType(IntType))),
        List(ForeignKey("DD_CONSTRAINTS", "DD_ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTES", "ATTRIBUTE_ID"))),
          ForeignKey("DD_CONSTRAINTS", "DD_ATTRIBUTES", List(("REF_TABLE_ID", "TABLE_ID"), ("REF_ATTRIBUTES", "ATTRIBUTE_ID"))),
          NotNull(constraintType),
          NotNull(attributes)))
    }
    val sequencesTable = {
      val startValue: Attribute = "START_VALUE" -> IntType
      val endValue: Attribute = "END_VALUE" -> IntType
      val incrementBy: Attribute = "INCREMENT_BY" -> IntType
      val name: Attribute = "NAME" -> StringType
      val sequenceId: Attribute = "SEQUENCE_ID" -> IntType

      new Table("DD_SEQUENCES", List(
        startValue,
        endValue,
        incrementBy,
        name,
        sequenceId),
        List(PrimaryKey(List(sequenceId)),
          Unique(name),
          NotNull(startValue),
          NotNull(endValue),
          NotNull(incrementBy)))
    }

    List(tablesTable, attributesTable, rowsTable, fieldsTable, constraintsTable, sequencesTable)
  }
}

case class Catalog(schemata: Map[String, Schema]) {

  /* Lists that contain the data in the data dictionary*/
  var ddTables: List[DDTablesRecord] = List.empty
  var ddAttributes: List[DDAttributesRecord] = List.empty
  var ddRows: List[DDRowsRecord] = List.empty
  var ddFields: List[DDFieldsRecord] = List.empty
  var ddConstraints: List[DDConstraintsRecord] = List.empty
  var ddSequences: List[DDSequencesRecord] = List.empty

  /** Initializes the data dictionary with itself */
  private def initializeDD() = {

    //TODO Use constructSequenceName
    /* Create initial sequences for the DD relations */
    ddSequences :+= DDSequencesRecord(1, Int.MaxValue, 1, DataDictionarySchemaName + "_DD_SEQUENCES_SEQUENCE_ID_SEQ", this, Some(0))
    ddSequences :+= DDSequencesRecord(0, Int.MaxValue, 1, DataDictionarySchemaName + "_DD_TABLES_TABLE_ID_SEQ", this)
    ddSequences :+= DDSequencesRecord(0, Int.MaxValue, 1, DataDictionarySchemaName + "_DD_ATTRIBUTES_ATTRIBUTE_ID_SEQ", this)
    ddSequences :+= DDSequencesRecord(0, Int.MaxValue, 1, DataDictionarySchemaName + "_DD_ROWS_ROW_ID_SEQ", this)

    /* Fill DD_TABLES */
    (0 until dataDictionary.size) foreach { i =>
      val tbl = dataDictionary(i)
      ddTables :+= DDTablesRecord(DataDictionarySchemaName, tbl.name, this)
    }

    /* Fill DD_ROWS with tables and DD_ATTRIBUTES for each table */
    (0 until dataDictionary.size) foreach { i =>
      val tbl = dataDictionary(i)
      ddRows :+= DDRowsRecord(i, this)

      (0 until tbl.attributes.size) foreach { j =>
        ddAttributes :+= DDAttributesRecord(i, attr.name, attr.dataType, this)
        ddRows :+= DDRowsRecord(1, this)
      }
    }

    //TODO Fill ddRows with data from all the DD relations
    //TODO Fill ddFields with data from all the DD relations
    //TODO Fill ddConstraints
    //TODO Fill ddSequences
  }

  /** Adds a table to the given schema */
  private def addTableToDD(schemaName: String, tbl: Table) = {
    if (tableExistsAlreadyInDD(schemaName, tbl.name)) {
      throw new Exception("Table " + tbl.name + " already exists in schema" + schemaName + ".")
    }

    /* Add entry to DD_TABLES*/
    val newTableId = getSequenceNext(constructSequenceName(DataDictionarySchemaName, "DD_TABLES", "TABLE_ID"))
    ddTables :+= DDTablesRecord(schemaName, tbl.name, this, Some(newTableId))
    addRowAndFieldsToDD(DataDictionarySchemaName, "DD_TABLES", Seq(schemaName, tbl.name, newTableId))

    /* Add entries to DD_ATTRIBUTES */
    val newAttributes: List[DDAttributesRecord] = for (attr <- tbl.attributes) yield DDAttributesRecord(newTableId, attr.name, attr.dataType, this)
    ddAttributes ++= newAttributes
    newAttributes.foreach(attr => addRowAndFieldsToDD(DataDictionarySchemaName, "DD_ATTRIBUTES", Seq(newTableId, attr.name, attr.dataType, attr.attributeId)))

    /* Add entries to DD_CONSTRAINTS */
    def filterAutoIncrement(cstr: Constraint): Boolean = {
      cstr match {
        case _: AutoIncrement => true
        case _                => false
      }
    }
    def filterNotAutoIncrement(cstr: Constraint) = !filterAutoIncrement

    val newConstraints = for (cstr <- tbl.constraints.filter(filterNotAutoIncrement)) yield cstr.toDDConstraintRecord
    ddConstraints ++= newConstraints
    //TODO How to handle SeqTypes or options like in DD_CONSTRAINTS
    newConstraints.foreach(cstr => addRowAndFieldsToDD(DataDictionarySchemaName, "DD_CONSTRAINTS", Seq()))

    /* Add entries to DD_SEQUENCES */
    val newSequences = for (cstr <- tbl.constraints.filter(filterAutoIncrement))
      yield DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DataDictionarySchemaName, tbl.Name, cstr.attribute), this)
    ddSequences ++= newSequences
    newSequences.foreach(seq => addRowAndFieldsToDD(DataDictionarySchemaName, "DD_SEQUENCES", Seq(seq.startValue, seq.endValue, seq.incrementBy, seq.sequenceName, seq.sequenceId)))
  }

  /* Populate DD with schemata that have been passed to the constructor */
  schemata.foreach {
    case (name, Schema(tables)) => tables.foreach { t =>
      addTableToDD(name, t)
    }
  }

  /** Returns the next value of the sequence with the given name */
  def getSequenceNext(sequenceName: String): Int = //TODO When there's a way to fetch data from the DB, replace this
    ddSequences.find(s => s.sequenceName == sequenceName) match {
      case Some(seq) => seq.nextVal
      case None      => throw new Exception(s"Sequence $sequenceName not found")
    }

  /** Returns whether a table with the given name already exists in the given schema */
  private def tableExistsAlreadyInDD(schemaName: String, tableName: String): Boolean =
    ddTables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case None => False
      case _    => True
    }

  /**
   * Retrieve a List of attributes of a table
   *
   * The attributes are ordered the same way they have been added to DD_ATTRIBUTES
   */
  private def getAttributes(schemaName: String, tableName: String): List[Attributes] = {
    val tableId = ddTables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case Some(tbl) => tbl.tableId
      case None      => throw new Exception(s"Table $tableName does not exist in schema $schemaName")
    }

    ddAttributes.filter(a => a.tableId == tableId)
  }
  /**
   * Adds a row and it's field values to the database
   *
   * @param values A sequence of values that should be stored.
   * The order must be the same as the attributes returned by the getAttributes-function.
   */
  private def addRowAndFieldsToDD(schemaName: String, tableName: String, values: Seq[Any]) = ???
  /* 
     * Take this as inspiration:
     val newRow = DDRowsRecord(0)
        ddRows += newRow
        ddFields ++= for ( (att, val) <- getAttributes(DataDictionarySchemaName, "DD_TABLES").zip(Seq(schemaName, tbl.name, newTableId))) 
                      yield DDFieldsRecord(newTableId, att.attributeId, newRow.rowId, val)
     */
  //TODO How to handle SeqTypes or options like in DD_CONSTRAINTS

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
