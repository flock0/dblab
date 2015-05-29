package ch.epfl.data
package dblab.legobase
package schema

import scala.collection.mutable.ArrayBuffer
import sc.pardis.types._
import schema._
import Catalog._
import storagemanager.Loader
/**
  * A catalog of one or multiple table schemata and may also contain their data
  */
object Catalog {
  /** The standardized sequence name */
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
        "REF_TABLE_NAME" -> OptionType(IntType),
        "REF_ATTRIBUTES" -> OptionType(SeqType(IntType))),
        List(ForeignKey("DD_CONSTRAINTS", "DD_ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTES", "ATTRIBUTE_ID"))),
          ForeignKey("DD_CONSTRAINTS", "DD_TABLES", List(("REF_TABLE_NAME", "NAME"))),
          ForeignKey("DD_CONSTRAINTS", "DD_ATTRIBUTES", List(("REF_ATTRIBUTES", "ATTRIBUTE_ID"))),
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

/**
  * Represents a catalog of one or multiple table schemata and contains their data
  *
  * @param schemata A map from the schema name to the schema itself
  */
case class Catalog(schemata: Map[String, Schema]) {

  /* Collections that contain the data in the data dictionary (and thus in the whole catalog) */
  private[schema] val ddTables: ArrayBuffer[DDTablesRecord] = ArrayBuffer.empty
  private[schema] val ddAttributes: ArrayBuffer[DDAttributesRecord] = ArrayBuffer.empty
  private[schema] val ddRows: ArrayBuffer[DDRowsRecord] = ArrayBuffer.empty
  private[schema] val ddFields: ArrayBuffer[DDFieldsRecord] = ArrayBuffer.empty
  private[schema] val ddConstraints: ArrayBuffer[DDConstraintsRecord] = ArrayBuffer.empty
  private[schema] val ddSequences: ArrayBuffer[DDSequencesRecord] = ArrayBuffer.empty

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
    ddSequences += DDSequencesRecord(1, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_SEQUENCES", "SEQUENCE_ID"), this, Some(0))
    ddSequences += DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"), this)
    ddSequences += DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID"), this)
    ddSequences += DDSequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_ROWS", "ROW_ID"), this)
    val initialSequences = ddSequences

    /* Fill DD_TABLES */
    (0 until dataDictionary.size) foreach { i =>
      val tbl = dd(i)
      ddTables += DDTablesRecord(DDSchemaName, tbl.name, this)

      /* For each table in the DD, add a row in DD_TABLES */
      val tableRow = DDRowsRecord(0, this)
      ddRows += tableRow

      /* Insert values for all rows in DD_TABLES */
      ddFields += DDFieldsRecord(0, 0, tableRow.rowId, ddTables(i).schemaName)
      ddFields += DDFieldsRecord(0, 1, tableRow.rowId, ddTables(i).name)
      ddFields += DDFieldsRecord(0, 2, tableRow.rowId, ddTables(i).fileName)
      ddFields += DDFieldsRecord(0, 3, tableRow.rowId, ddTables(i).tableId)
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = ddTables(i)

      tbl.attributes foreach { attr =>
        /* Insert attributes for all tables */
        val newAttribute = DDAttributesRecord(i, attr.name, attr.dataType, this)
        ddAttributes += newAttribute
        /* Insert rows for DD_ATTRIBUTES */
        val attributesRow = DDRowsRecord(1, this)
        ddRows += attributesRow
        /* Insert records for each attribute into DD_FIELDS */
        ddFields += DDFieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.tableId)
        ddFields += DDFieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.name)
        ddFields += DDFieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.dataType)
        ddFields += DDFieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.attributeId)
      }
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = ddTables(i)

      val constraints = tbl.constraints.filter(filterNotAutoIncrement)
      (0 until constraints.size) foreach { j =>
        /* Insert constraints for all tables */
        val newConstraint = constraints(j).toDDConstraintsRecord(this, tblRecord.tableId)
        ddConstraints += newConstraint
        /* Insert rows for DD_CONSTRAINTS */
        val constraintRow = DDRowsRecord(4, this)
        ddRows += constraintRow

        /* Insert records for each constraint into DD_FIELDS */
        val constraintAttributes = ddAttributes.filter(_.tableId == 4)
        ddFields += DDFieldsRecord(4, constraintAttributes(0).attributeId, constraintRow.rowId, newConstraint.tableId)
        ddFields += DDFieldsRecord(4, constraintAttributes(1).attributeId, constraintRow.rowId, newConstraint.constraintType)
        ddFields += DDFieldsRecord(4, constraintAttributes(2).attributeId, constraintRow.rowId, newConstraint.attributes)
        ddFields += DDFieldsRecord(4, constraintAttributes(3).attributeId, constraintRow.rowId, newConstraint.refTableName)
        ddFields += DDFieldsRecord(4, constraintAttributes(4).attributeId, constraintRow.rowId, newConstraint.refAttributes)
      }
    }

    initialSequences foreach { seq =>
      /* Insert rows for DD_SEQUENCES */
      val sequenceRow = DDRowsRecord(5, this)
      ddRows += sequenceRow

      /* Insert records for each sequence into DD_FIELDS */
      val sequenceAttributes = ddAttributes.filter(_.tableId == 5)
      ddFields += DDFieldsRecord(5, sequenceAttributes(0).attributeId, sequenceRow.rowId, seq.startValue)
      ddFields += DDFieldsRecord(5, sequenceAttributes(1).attributeId, sequenceRow.rowId, seq.endValue)
      ddFields += DDFieldsRecord(5, sequenceAttributes(2).attributeId, sequenceRow.rowId, seq.incrementBy)
      ddFields += DDFieldsRecord(5, sequenceAttributes(3).attributeId, sequenceRow.rowId, seq.sequenceName)
      ddFields += DDFieldsRecord(5, sequenceAttributes(4).attributeId, sequenceRow.rowId, seq.sequenceId)
    }
  }

  /** 
    * Adds a table to the given schema
    * 
    * Please note: This does not load the tables data into the catalog
    */
  private def addTableToDD(schemaName: String, tbl: Table) = {
    if (tableExistsAlreadyInDD(schemaName, tbl.name)) {
      throw new Exception(s"Table ${tbl.name} already exists in schema $schemaName.")
    }

    /* Add entry to DD_TABLES*/
    val newTableId = getSequenceNext(constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"))
    ddTables += DDTablesRecord(schemaName, tbl.name, this, Some(tbl.fileName), Some(newTableId))
    addTuple(DDSchemaName, "DD_TABLES", Seq(schemaName, tbl.name, newTableId))

    /* Add entries to DD_ATTRIBUTES */
    val newAttributes: List[DDAttributesRecord] = for (attr <- tbl.attributes) yield DDAttributesRecord(newTableId, attr.name, attr.dataType, this)
    ddAttributes ++= newAttributes
    newAttributes.foreach(attr => addTuple(DDSchemaName, "DD_ATTRIBUTES", Seq(newTableId, attr.name, attr.dataType, attr.attributeId)))

    /* Add entries to DD_CONSTRAINTS */
    val newConstraints = for (cstr <- tbl.constraints.filter(filterNotAutoIncrement)) yield cstr.toDDConstraintsRecord(this, newTableId)
    ddConstraints ++= newConstraints
    newConstraints.foreach(cstr => addTuple(DDSchemaName, "DD_CONSTRAINTS", Seq(newTableId, cstr.constraintType, cstr.attributes, cstr.refTableName, cstr.refAttributes)))

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

  /**
    * Adds a row and it's field values to the database
    *
    * @param schemaName The name of the schema where the table resides
    * @param tableName The name of the table to add the tuple to
    * @param values A sequence of values that should be stored.
    * The order of the sequence must correspond to the order of the attributes in the data dictionary
    */
  def addTuple(schemaName: String, tableName: String, values: Seq[Any]): Unit = {
    val tableId = getTable(schemaName, tableName).tableId
    val attributeIds = getAttributes(tableId).map(_.attributeId)
    addTuple(tableId, attributeIds zip values)
  }

  /**
    * Adds a row and it's field values to the database
    *
    * @param tableId The id of the table to add the tuple to
    * @param values A sequence of values that should be stored.
    * The order of the sequence must correspond to the order of the attributes in the data dictionary
    */
  def addTuple(tableId: Int, values: Seq[(Int, Any)]): Unit = {

    val row = DDRowsRecord(tableId, this)
    ddRows += row

    val records: Seq[DDFieldsRecord] = for ((attrId, value) <- values)
      yield DDFieldsRecord(tableId, attrId, row.rowId, value)
    ddFields ++= records
  }

  /**
    * Returns all tuples for the requested table
    *
    * @param schemaName The name of the schema where this table resides
    * @param tableName The name of the table
    * @return An array of all records for this table
    */
  def getTuples(schemaName: String, tableName: String): Array[Record] =
    getTuples(getTable(schemaName, tableName))

  /**
    * Returns all tuples for the requested table
    *
    * @param tableId The id of the table to get the tuples for
    * @return An array of all records for this table
    */
  def getTuples(tableId: Int): Array[Record] =
    getTuples(getTable(tableId))

  /**
    * Returns all tuples for the requested table
    *
    * @param table The table to get the tuples for
    * @return An array of all records for this table
    */
  private def getTuples(table: DDTablesRecord): Array[Record] = {
    if (!isDataDictionary(table)) /* Only load tables from disk that are not part of the data dictionary */
      Loader.loadTable(this, table)
    ddRows.filter(row => row.tableId == table.tableId).map(row => Record(this, table.tableId, row.rowId)).toArray
  }

  /** Returns the attribute with the specified name in the given table */
  def getAttribute(tableId: Int, attributeName: String) = getAttributes(tableId, List(attributeName)).head

  /** Returns all attributes of the specified table */
  def getAttributes(tableId: Int) = ddAttributes.filter(a => tableId == a.tableId)

  /** Returns a list of attributes with the specified names that belong to the given table */
  def getAttributes(tableId: Int, attributeNames: List[String]) = {
    val attributes = ddAttributes.filter(at => at.tableId == tableId && attributeNames.contains(at.name)).toList

    if (attributes.size != attributeNames.size)
      throw new Exception(s"Couldn't find all requested attributes in $tableId")
    attributes
  }

  /** Returns the table with specified name in the given schema */
  def getTable(schemaName: String, tableName: String): DDTablesRecord = {
    ddTables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case Some(t) => t
      case None    => throw new Exception(s"Table $tableName does not exist in schema $schemaName")
    }
  }

  /** Returns the table with the specified tableId */
  def getTable(tableId: Int) =
    ddTables.find(tbl => tbl.tableId == tableId) match {
      case Some(t) => t
      case None    => throw new Exception(s"Table with ID $tableId doesn't exist in this catalog")
    }

  /** Returns the field identified by the given IDs */
  private[schema] def getField[T](tableId: Int, attributeId: Int, rowId: Int): Option[T] =
    ddFields.find(f => f.tableId == tableId && f.attributeId == attributeId && f.rowId == rowId) match {
      case Some(rec) => Some(rec.value.asInstanceOf[T])
      case None      => None
    }

  /** Indicates whether a constraint is an AutoIncrement (as they are handled seperately) */
  private def filterAutoIncrement(cstr: Constraint): Boolean = {
    cstr match {
      case _: AutoIncrement => true
      case _                => false
    }
  }

  private def filterNotAutoIncrement(cstr: Constraint) = !filterAutoIncrement(cstr)

  /** Returns the next value of the sequence with the given name */
  private[schema] def getSequenceNext(sequenceName: String): Int = //TODO When there's a way to fetch data from the DB, replace this
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

  /** Indicates whether a table belongs to the data dictionary */
  private def isDataDictionary(table: DDTablesRecord): Boolean = table.schemaName == DDSchemaName

  /** Returns whether the given row exists in the given table */
  private[schema] def rowExists(tableId: Int, rowId: Int): Boolean =
    ddRows.find(r => r.rowId == rowId && r.tableId == tableId) match {
      case None => false
      case _    => true
    }
}