package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions
import Catalog._

/* Case classes for the tables in the data dictionary */
case class DDTablesRecord(schemaName: String, name: String, private val catalog: Catalog, val fileName: Option[String] = None, private val _tableId: Option[Int] = None) {
  val tableId = _tableId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"))
  }
}
case class DDAttributesRecord(tableId: Int, name: String, dataType: Tpe, private val catalog: Catalog, private val _attributeId: Option[Int] = None) {
  val attributeId = _attributeId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID"))
  }
}
case class DDFieldsRecord(tableId: Int, attributeId: Int, rowId: Int, value: Any)
case class DDRowsRecord(tableId: Int, private val catalog: Catalog, private val _rowId: Option[Int] = None) {
  val rowId = _rowId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_ROWS", "ROW_ID"))
  }
}
case class DDConstraintsRecord(tableId: Int, constraintType: Char, attributes: List[Int], refTableId: Option[Int], refAttributes: Option[List[Int]])
case class DDSequencesRecord(startValue: Int, endValue: Int, incrementBy: Int, sequenceName: String, private val catalog: Catalog, private val _sequenceId: Option[Int] = None) {
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

  val sequenceId = _sequenceId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_SEQUENCES", "SEQUENCE_ID"))
  }

  private var next = startValue

  /**
   * Returns the next value of the sequence
   *
   * @return The next value of the sequence
   * @throws Exception when the sequence has been exhausted
   */
  def nextVal: Int = {
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
  val DDSchemaName = "DD"

  /** The tables that comprise the data dictionary */
  private val dataDictionary: List[Table] = {
    val tablesTable = {
      val schemaName: Attribute = "SCHEMA_NAME" -> StringType
      val name: Attribute = "NAME" -> StringType
      val tableId: Attribute = "TABLE_ID" -> IntType

      new Table("DD_TABLES", List(
        schemaName,
        name,
        "FILENAME" -> StringType,
        tableId),
        List(PrimaryKey(List(tableId)),
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

  initializeDD()
  /* Populate DD with schemata that have been passed to the constructor */
  schemata.foreach {
    case (name, Schema(tables)) => tables.foreach { t =>
      addTableToDD(name, t)
    }
  }

  /**
   * Initializes the data dictionary with itself
   *
   * We avoid self-referencing entries in the DD_ROWS and DD_FIELDS relations.
   */
  private def initializeDD() = {
    val dd = dataDictionary
    /* Create initial sequences for the DD relations */
    ddSequences :+= DDSequencesRecord(1, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_SEQUENCES", "SEQUENCE_ID"), this, Some(0))
    ddSequences :+= DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"), this)
    ddSequences :+= DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID"), this)
    ddSequences :+= DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_ROWS", "ROW_ID"), this)
    val initialSequences = ddSequences

    /* Fill DD_TABLES */
    (0 until dataDictionary.size) foreach { i =>
      val tbl = dd(i)
      ddTables :+= DDTablesRecord(DDSchemaName, tbl.name, this)

      /* For each table in the DD, add a row in DD_TABLES */
      val tableRow = DDRowsRecord(0, this)
      ddRows :+= tableRow

      /* Insert values for all rows in DD_TABLES */
      (0 until dd(0).attributes.size) foreach { j =>
        ddFields :+= DDFieldsRecord(0, j, tableRow.rowId, ddTables(i).productElement(j))
      }
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = ddTables(i)

      tbl.attributes foreach { attr =>
        /* Insert attributes for all tables */
        val newAttribute = DDAttributesRecord(i, attr.name, attr.dataType, this)
        ddAttributes :+= newAttribute
        /* Insert rows for DD_ATTRIBUTES */
        val attributesRow = DDRowsRecord(1, this)
        ddRows :+= attributesRow
        /* Insert records for each attribute into DD_VALUES */
        (0 until dd(2).attributes.size) foreach { attrIndex =>
          ddFields :+= DDFieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.productElement(attrIndex))
        }
      }
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = ddTables(i)

      val constraints = tbl.constraints.filter(filterNotAutoIncrement)
      (0 until constraints.size) foreach { j =>
        /* Insert constraints for all tables */
        val newConstraint = constraints(j).toDDConstraintsRecord(this, tblRecord.tableId)
        ddConstraints :+= newConstraint
        /* Insert rows for DD_CONSTRAINTS */
        val constraintRow = DDRowsRecord(4, this)
        ddRows :+= constraintRow

        /* Insert records for each constraint into DD_VALUES */
        val constraintAttributes = ddAttributes.filter(_.tableId == 4)
        (0 until constraintAttributes.size) foreach { k =>
          ddFields :+= DDFieldsRecord(4, constraintAttributes(k).attributeId, constraintRow.rowId, newConstraint.productElement(k))
        }
      }

      initialSequences foreach { seq =>
        /* Insert rows for DD_SEQUENCES */
        val sequenceRow = DDRowsRecord(5, this)
        ddRows :+= sequenceRow

        /* Insert records for each sequence into DD_VALUES */
        val sequenceAttributes = ddAttributes.filter(_.tableId == 5)
        (0 until sequenceAttributes.size) foreach { k =>
          ddFields :+= DDFieldsRecord(5, sequenceAttributes(k).attributeId, sequenceRow.rowId, seq.productElement(k))
        }
      }
    }
  }

  /** Adds a table to the given schema */
  private def addTableToDD(schemaName: String, tbl: Table) = {
    if (tableExistsAlreadyInDD(schemaName, tbl.name)) {
      throw new Exception("Table " + tbl.name + " already exists in schema" + schemaName + ".")
    }

    /* Add entry to DD_TABLES*/
    val newTableId = getSequenceNext(constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"))
    ddTables :+= DDTablesRecord(schemaName, tbl.name, this, Some(tbl.resourceLocator), Some(newTableId))
    addTuple(DDSchemaName, "DD_TABLES", Seq(schemaName, tbl.name, newTableId))
    
    /* Add entries to DD_ATTRIBUTES */
    val newAttributes: List[DDAttributesRecord] = for (attr <- tbl.attributes) yield DDAttributesRecord(newTableId, attr.name, attr.dataType, this)
    ddAttributes ++= newAttributes
    newAttributes.foreach(attr => addTuple(DDSchemaName, "DD_ATTRIBUTES", Seq(newTableId, attr.name, attr.dataType, attr.attributeId)))

    /* Add entries to DD_CONSTRAINTS */
    val newConstraints = for (cstr <- tbl.constraints.filter(filterNotAutoIncrement)) yield cstr.toDDConstraintsRecord(this, newTableId)
    ddConstraints ++= newConstraints
    newConstraints.foreach(cstr => addTuple(DDSchemaName, "DD_CONSTRAINTS", Seq(newTableId, cstr.constraintType, cstr.attributes, cstr.refTableId, cstr.refAttributes)))

    /* Add entries to DD_SEQUENCES */
    val newSequences: Seq[DDSequencesRecord] = for (cstr <- tbl.constraints.filter(filterAutoIncrement)) yield {
      cstr match {
        case ai: AutoIncrement => DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, tbl.name, ai.attribute.name), this)
        case _                 => throw new ClassCastException
      }

    }
    ddSequences ++= newSequences
    newSequences.foreach(seq => addTuple(DDSchemaName, "DD_SEQUENCES", Seq(seq.startValue, seq.endValue, seq.incrementBy, seq.sequenceName, seq.sequenceId)))
  }

  /** Indicates whether a constraint is an AutoIncrement as theye are handled seperately */
  private def filterAutoIncrement(cstr: Constraint): Boolean = {
    cstr match {
      case _: AutoIncrement => true
      case _                => false
    }
  }

  private def filterNotAutoIncrement(cstr: Constraint) = !filterAutoIncrement(cstr)

  /** Returns the next value of the sequence with the given name */
  def getSequenceNext(sequenceName: String): Int = //TODO When there's a way to fetch data from the DB, replace this
    ddSequences.find(s => s.sequenceName == sequenceName) match {
      case Some(seq) => seq.nextVal
      case None      => throw new Exception(s"Sequence $sequenceName not found")
    }

  /** Returns whether a table with the given name already exists in the given schema */
  private def tableExistsAlreadyInDD(schemaName: String, tableName: String): Boolean =
    ddTables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case None => false
      case _    => true
    }

  private def getTableId(schemaName: String, tableName: String): Int = {
    ddTables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case Some(t) => t.tableId
      case None    => throw new Exception(s"Table $tableName does not exist in schema $schemaName")
    }
  }

  /**
   * Adds a row and it's field values to the database
   *
   * @param values A sequence of values that should be stored.
   * The order of the sequence correspond to the order of the attributes in the data dictionary
   */
  def addTuple(schemaName: String, tableName: String, values: Seq[Any]): Unit = {

    def getAttributeIds(tId: Int) = ddAttributes.filter(a => a.tableId == tId).map(_.attributeId)
    val tableId = getTableId(schemaName, tableName)
    addTuple(tableId, getAttributeIds(tableId) zip values)
  }

  /**
   * Adds a row and it's field values to the database
   *
   * @param values n iterable with the attributeId and the value to be stored for that attribute.
   */
  def addTuple(tableId: Int, values: Seq[(Int, Any)]): Unit = {

    val row = DDRowsRecord(tableId, this)
    ddRows :+= row

    val records: Seq[DDFieldsRecord] = for ((attrId, value) <- values)
      yield DDFieldsRecord(tableId, attrId, row.rowId, value)
    ddFields ++= records
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
        case None    => throw new Exception("Couldn't find attribute " + attributeName + " in table " + tID) //TODO Edit message
      }
    val schemaName = catalog.ddTables.find(_.tableId == tableId) match {
      case Some(t) => t.schemaName
      case None    => throw new Exception(s"Couldn't find schema of table with ID $tableId")
    }

    val refTableId = catalog.ddTables.find(t => t.name == referencedTable && t.schemaName == schemaName) match {
      case Some(t) => t.tableId
      case None    => throw new Exception(s"Couldn't find schema of table with ID $tableId")
    }

    val attributeIds: List[(Int, Int)] = attributes map { case (loc: String, ref: String) => findAttributeId(tableId, loc) -> findAttributeId(refTableId, ref) }
    val (attributesList, refAttributesList) = attributeIds.unzip
    DDConstraintsRecord(
      tableId,
      'f',
      attributesList,
      Some(refTableId),
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
      case None    => throw new Exception("Couldn't find attribute " + attribute.name)
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
      case None    => throw new Exception("Couldn't find attribute " + attribute.name)
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
