package ch.epfl.data
package dblab.legobase
package schema.datadict

import scala.collection.mutable.{ ArrayBuffer, Map }
import sc.pardis.types._
import schema.{ DateType, VarCharType, TableType, TpeType, Statistics }
import helper._
import DataDictionary._
import storagemanager.Loader
/**
 * A catalog of one or multiple table schemata that may also contain their data
 */
object DataDictionary {
  /** The standardized sequence name */
  def constructSequenceName(schemaName: String, tableName: String, attributeName: String) =
    constructSchemaTableSequenceNamePart(schemaName, tableName) + attributeName + "_SEQ"

  /** The part of the sequence name that denotes the schema and table this sequence belongs to */
  def constructSchemaTableSequenceNamePart(schemaName: String, tableName: String) = schemaName + "_" + tableName + "_"

  /** The default name of the schema for the data dictionary itself */
  val DDSchemaName = "DD"

  /** The tables that comprise the data dictionary */
  private val dataDictionary: List[Table] = {
    val tablesTable = {
      val schemaName: Attribute = "SCHEMA_NAME" -> StringType
      val name: Attribute = "NAME" -> StringType
      val tableId: Attribute = "TABLE_ID" -> IntType

      new Table("TABLES", List(
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

      new Table("ATTRIBUTES", List(
        tableId,
        name,
        datatype,
        attributeId),
        List(PrimaryKey(List(tableId, attributeId)),
          ForeignKey("ATTRIBUTES", "TABLES", List(("TABLE_ID", "TABLE_ID"))),
          NotNull(name),
          NotNull(datatype),
          AutoIncrement(attributeId)))
    }
    val rowsTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val rowId: Attribute = "ROW_ID" -> IntType

      new Table("ROWS", List(
        tableId,
        rowId),
        List(PrimaryKey(List(tableId, rowId)),
          ForeignKey("ROWS", "TABLES", List(("TABLE_ID", "TABLE_ID"))),
          AutoIncrement(rowId)))
    }
    val fieldsTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val attributeId: Attribute = "ATTRIBUTE_ID" -> IntType
      val rowId: Attribute = "ROW_ID" -> IntType
      val value: Attribute = "VALUE" -> AnyType

      new Table("FIELDS", List(
        tableId,
        attributeId,
        rowId,
        value),
        List(PrimaryKey(List(tableId, attributeId, rowId)),
          ForeignKey("FIELDS", "ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTE_ID", "ATTRIBUTE_ID"))),
          ForeignKey("FIELDS", "ROWS", List(("TABLE_ID", "TABLE_ID"), ("ROW_ID", "ROW_ID"))),
          NotNull(value)))
    }
    val constraintsTable = {
      val tableId: Attribute = "TABLE_ID" -> IntType
      val constraintType: Attribute = "CONSTRAINT_TYPE" -> CharType /* f = foreign key constraint, p = primary key constraint, u = unique constraint, n = notnull constraint, c = compressed */
      val attributes: Attribute = "ATTRIBUTES" -> SeqType(IntType)
      val foreignKeyName: Attribute = "FOREIGN_KEY_NAME" -> OptionType(StringType)

      new Table("CONSTRAINTS", List(
        tableId,
        constraintType,
        attributes,
        "REF_TABLE_NAME" -> OptionType(IntType),
        foreignKeyName),
        List(ForeignKey("CONSTRAINTS", "ATTRIBUTES", List(("TABLE_ID", "TABLE_ID"), ("ATTRIBUTES", "ATTRIBUTE_ID"))),
          ForeignKey("CONSTRAINTS", "TABLES", List(("REF_TABLE_NAME", "NAME"))),
          ForeignKey("CONSTRAINTS", "ATTRIBUTES", List(("REF_ATTRIBUTES", "ATTRIBUTE_ID"))),
          NotNull(constraintType),
          NotNull(attributes),
          Unique(foreignKeyName)))
    }
    val sequencesTable = {
      val startValue: Attribute = "START_VALUE" -> IntType
      val endValue: Attribute = "END_VALUE" -> IntType
      val incrementBy: Attribute = "INCREMENT_BY" -> IntType
      val name: Attribute = "NAME" -> StringType
      val sequenceId: Attribute = "SEQUENCE_ID" -> IntType

      new Table("SEQUENCES", List(
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
case class DataDictionary() {

  /* Collections that contain the data in the data dictionary (and thus in the whole catalog) */
  private[datadict] val tables: ArrayBuffer[TablesRecord] = ArrayBuffer.empty
  private[datadict] val attributes: ArrayBuffer[AttributesRecord] = ArrayBuffer.empty
  private[datadict] val rows: ArrayBuffer[RowsRecord] = ArrayBuffer.empty
  private[datadict] val fields: ArrayBuffer[FieldsRecord] = ArrayBuffer.empty
  private[datadict] val constraints: ArrayBuffer[ConstraintsRecord] = ArrayBuffer.empty
  private[datadict] val sequences: ArrayBuffer[SequencesRecord] = ArrayBuffer.empty
  private[datadict] val stats: Map[String, Statistics] = Map.empty //TODO Move to data dictionary

  initializeDD()

  /**
   * Initializes the data dictionary with itself
   *
   * We avoid self-referencing entries in the ROWS and FIELDS relations.
   */
  private def initializeDD() = {
    val dd = dataDictionary
    /* Create initial sequences for the DD relations */
    sequences += SequencesRecord(1, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "SEQUENCES", "SEQUENCE_ID"), this, Some(0))
    sequences += SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "TABLES", "TABLE_ID"), this)
    sequences += SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "ATTRIBUTES", "ATTRIBUTE_ID"), this)
    sequences += SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "ROWS", "ROW_ID"), this)
    val initialSequences = sequences

    /* Fill TABLES */
    (0 until dataDictionary.size) foreach { i =>
      val tbl = dd(i)
      tables += TablesRecord(DDSchemaName, tbl.name, this)

      /* For each table in the DD, add a row in TABLES */
      val tableRow = RowsRecord(0, this)
      rows += tableRow

      /* Insert values for all rows in TABLES */
      fields += FieldsRecord(0, 0, tableRow.rowId, tables(i).schemaName)
      fields += FieldsRecord(0, 1, tableRow.rowId, tables(i).name)
      fields += FieldsRecord(0, 2, tableRow.rowId, tables(i).fileName)
      fields += FieldsRecord(0, 3, tableRow.rowId, tables(i).tableId)
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = tables(i)

      tbl.attributes foreach { attr =>
        /* Insert attributes for all tables */
        val newAttribute = AttributesRecord(i, attr.name, attr.dataType, this)
        attributes += newAttribute
        /* Insert rows for ATTRIBUTES */
        val attributesRow = RowsRecord(1, this)
        rows += attributesRow
        /* Insert records for each attribute into FIELDS */
        fields += FieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.tableId)
        fields += FieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.name)
        fields += FieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.dataType)
        fields += FieldsRecord(1, newAttribute.attributeId, attributesRow.rowId, newAttribute.attributeId)
      }
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = tables(i)

      val tblConstraints = tbl.constraints.filter(filterNotAutoIncrement)
      (0 until tblConstraints.size) foreach { j =>
        /* Insert constraints for all tables */
        val newConstraint = tblConstraints(j).toConstraintsRecord(this, tblRecord.tableId)
        constraints += newConstraint
        /* Insert rows for CONSTRAINTS */
        val constraintRow = RowsRecord(4, this)
        rows += constraintRow

        /* Insert records for each constraint into FIELDS */
        val constraintAttributes = attributes.filter(_.tableId == 4)
        fields += FieldsRecord(4, constraintAttributes(0).attributeId, constraintRow.rowId, newConstraint.tableId)
        fields += FieldsRecord(4, constraintAttributes(1).attributeId, constraintRow.rowId, newConstraint.constraintType)
        fields += FieldsRecord(4, constraintAttributes(2).attributeId, constraintRow.rowId, newConstraint.attributes)
        fields += FieldsRecord(4, constraintAttributes(3).attributeId, constraintRow.rowId, newConstraint.refTableName)
        fields += FieldsRecord(4, constraintAttributes(4).attributeId, constraintRow.rowId, newConstraint.refAttributes)
        fields += FieldsRecord(4, constraintAttributes(5).attributeId, constraintRow.rowId, newConstraint.foreignKeyName)
      }
    }

    initialSequences foreach { seq =>
      /* Insert rows for SEQUENCES */
      val sequenceRow = RowsRecord(5, this)
      rows += sequenceRow

      /* Insert records for each sequence into FIELDS */
      val sequenceAttributes = attributes.filter(_.tableId == 5)
      fields += FieldsRecord(5, sequenceAttributes(0).attributeId, sequenceRow.rowId, seq.startValue)
      fields += FieldsRecord(5, sequenceAttributes(1).attributeId, sequenceRow.rowId, seq.endValue)
      fields += FieldsRecord(5, sequenceAttributes(2).attributeId, sequenceRow.rowId, seq.incrementBy)
      fields += FieldsRecord(5, sequenceAttributes(3).attributeId, sequenceRow.rowId, seq.sequenceName)
      fields += FieldsRecord(5, sequenceAttributes(4).attributeId, sequenceRow.rowId, seq.sequenceId)
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

    /* Add entry to TABLES*/
    val newTableId = getSequenceNext(constructSequenceName(DDSchemaName, "TABLES", "TABLE_ID"))
    tables += TablesRecord(schemaName, tbl.name, this, Some(tbl.fileName), Some(newTableId))
    addTuple(DDSchemaName, "TABLES", Seq(schemaName, tbl.name, newTableId))

    /* Add entries to ATTRIBUTES */
    val newAttributes: List[AttributesRecord] = for (attr <- tbl.attributes) yield AttributesRecord(newTableId, attr.name, attr.dataType, this)
    attributes ++= newAttributes
    newAttributes.foreach(attr => addTuple(DDSchemaName, "ATTRIBUTES", Seq(newTableId, attr.name, attr.dataType, attr.attributeId)))

    /* Add entries to CONSTRAINTS */
    val newConstraints = for (cstr <- tbl.constraints.filter(filterNotAutoIncrement)) yield cstr.toConstraintsRecord(this, newTableId)
    constraints ++= newConstraints
    newConstraints.foreach(cstr => addTuple(DDSchemaName, "CONSTRAINTS", Seq(newTableId, cstr.constraintType, cstr.attributes, cstr.refTableName, cstr.refAttributes)))

    /* Add entries to SEQUENCES */
    val newSequences: Seq[SequencesRecord] = for (cstr <- tbl.constraints.filter(filterAutoIncrement)) yield {
      cstr match {
        case ai: AutoIncrement => SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, tbl.name, ai.attribute.name), this)
        case _                 => throw new ClassCastException
      }

    }
    sequences ++= newSequences
    newSequences.foreach(seq => addTuple(DDSchemaName, "SEQUENCES", Seq(seq.startValue, seq.endValue, seq.incrementBy, seq.sequenceName, seq.sequenceId)))
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

    val row = RowsRecord(tableId, this)
    rows += row

    val records: Seq[FieldsRecord] = for ((attrId, value) <- values)
      yield FieldsRecord(tableId, attrId, row.rowId, value)
    fields ++= records
  }

  /**
   * Returns all tuples for the requested table
   *
   * @param schemaName The name of the schema where this table resides
   * @param tableName The name of the table
   * @return An array of all records for this table
   */
  def getTuples(schemaName: String, tableName: String): Array[Tuple] =
    getTuples(getTable(schemaName, tableName))

  /**
   * Returns all tuples for the requested table
   *
   * @param tableId The id of the table to get the tuples for
   * @return An array of all records for this table
   */
  def getTuples(tableId: Int): Array[Tuple] =
    getTuples(getTable(tableId))

  /**
   * Returns all tuples for the requested table
   *
   * @param table The table to get the tuples for
   * @return An array of all records for this table
   */
  private[datadict] def getTuples(table: TablesRecord): Array[Tuple] = {
    if (!isDataDictionary(table)) /* Only load tables from disk that are not part of the data dictionary */
      Loader.loadTable(this, table)
    rows.filter(row => row.tableId == table.tableId).map(row => Tuple(this, table.tableId, row.rowId)).toArray
  }

  /** Returns the attribute with the specified name in the given table */
  def getAttribute(tableId: Int, attributeName: String) = getAttributes(tableId, List(attributeName)).head

  /** Returns the attribute with the specified id in the given table */
  def getAttribute(tableId: Int, attributeId: Int) = getAttributesFromIds(tableId, List(attributeId)).head

  /** Returns all attributes */
  def getAttributes(attrName: String) = attributes.filter(attrName == _.name)

  /** Returns all attributes of the specified table */
  def getAttributes(tableId: Int) = attributes.filter(a => tableId == a.tableId)

  /** Returns a list of attributes with the specified names that belong to the given table */
  def getAttributes(tableId: Int, attributeNames: List[String]) = {
    val atts = attributes.filter(at => at.tableId == tableId && attributeNames.contains(at.name)).toList

    if (atts.size != attributeNames.size)
      throw new Exception(s"Couldn't find all requested attributes in $tableId")
    atts
  }

  /** Returns a list of attributes with the specified names */
  def getAttributes(attributeNames: List[String]) = {
    val atts = attributes.filter(at => attributeNames.contains(at.name)).toList

    if (atts.size != attributeNames.size)
      throw new Exception(s"Couldn't find all requested attributes")
    atts
  }

  /** Returns a list of attributes with the specified ids that belong to the given table */
  def getAttributesFromIds(tableId: Int, attributeIds: List[Int]) = {
    val atts = attributes.filter(at => at.tableId == tableId && attributeIds.contains(at.attributeId)).toList

    if (atts.size != attributeIds.size)
      throw new Exception(s"Couldn't find all requested attributes in $tableId")
    atts
  }

  /** Returns the table with specified name in the given schema */
  def getTable(schemaName: String, tableName: String): TablesRecord =
    tables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case Some(t) => t
      case None    => throw new Exception(s"Table $tableName does not exist in schema $schemaName")
    }

  /** Returns the table with the specified tableId */
  def getTable(tableId: Int) =
    tables.find(_.tableId == tableId) match {
      case Some(t) => t
      case None    => throw new Exception(s"Table with ID $tableId doesn't exist in this catalog")
    }

  /** Returns the tables in the given schema */
  def getTables(schemaName: String): Seq[TablesRecord] = tables.filter(_.schemaName == schemaName)

  /** Returns the field identified by the given IDs */
  private[schema] def getField[T](tableId: Int, attributeId: Int, rowId: Int): Option[T] =
    fields.find(f => f.tableId == tableId && f.attributeId == attributeId && f.rowId == rowId) match {
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
    sequences.find(s => s.sequenceName == sequenceName) match {
      case Some(seq) => seq.nextVal
      case None      => throw new Exception(s"Sequence $sequenceName not found")
    }

  /** Returns whether a table with the given name already exists in the given schema */
  private def tableExistsAlreadyInDD(schemaName: String, tableName: String): Boolean =
    tables.find(t => t.schemaName == schemaName && t.name == tableName) match {
      case None => false
      case _    => true
    }

  /** Indicates whether a table belongs to the data dictionary */
  private def isDataDictionary(table: TablesRecord): Boolean = table.schemaName == DDSchemaName

  /** Returns whether the given row exists in the given table */
  private[schema] def rowExists(tableId: Int, rowId: Int): Boolean =
    rows.find(r => r.rowId == rowId && r.tableId == tableId) match {
      case None => false
      case _    => true
    }

  private[datadict] def getStats(schemaName: String): Statistics = stats.getOrElseUpdate(schemaName, Statistics())

  private[datadict] def addTable(schemaName: String, tableName: String, tableAttributes: Seq[(String, PardisType[_], Seq[schema.Constraint])], fileName: String) = {
    if (tableExistsAlreadyInDD(schemaName, tableName)) {
      throw new Exception(s"Table $tableName already exists in schema $schemaName.")
    }

    /* Add entry to TABLES*/
    val newTableId = getSequenceNext(constructSequenceName(DDSchemaName, "TABLES", "TABLE_ID"))
    tables += TablesRecord(schemaName, tableName, this, Some(fileName), Some(newTableId))
    addTuple(DDSchemaName, "TABLES", Seq(schemaName, tableName, newTableId))

    /* Add entries to ATTRIBUTES */
    val newAttributes: Seq[AttributesRecord] = for (attr <- tableAttributes) yield AttributesRecord(newTableId, attr._1, attr._2, this)
    attributes ++= newAttributes
    newAttributes.foreach(attr => addTuple(DDSchemaName, "ATTRIBUTES", Seq(newTableId, attr.name, attr.dataType, attr.attributeId)))

    /* We don't add the constraints here, as there's the seperate addConstraint()-method to do that */
  }

  private[datadict] def dropTable(schemaName: String, tableName: String) = {
    val table = getTable(schemaName, tableName)

    val start = constructSchemaTableSequenceNamePart(schemaName, tableName)
    sequences --= sequences.filter(s => s.sequenceName.startsWith(start))

    constraints --= constraints.filter(c => c.tableId == table.tableId)

    fields --= fields.filter(f => f.tableId == table.tableId)

    rows --= rows.filter(r => r.tableId == table.tableId)

    attributes --= attributes.filter(a => a.tableId == table.tableId)

    tables -= table
  }

  private[datadict] def getConstraints[T](tableId: Int, constraintType: Char) = {
    constraints.filter(c => c.tableId == tableId && c.constraintType == constraintType)
  }
  private[datadict] def getConstraints(tableId: Int) = constraints.filter(c => c.tableId == tableId)

  private[datadict] def getConstraints(attrName: String) = {
    val attrs = getAttributes(attrName)
    if (attrs.size == 0)
      Seq.empty
    else { // Assume attribute names are unique
      val at = attrs.head
      constraints.filter(c => c.tableId == at.tableId && c.attributes.contains(at.attributeId))
    }
  }

  private[datadict] def dropPrimaryKey(tableId: Int) = constraints --= getConstraints(tableId, 'p')

  private[datadict] def dropForeignKey(tableId: Int, foreignKeyName: String) =
    constraints --= constraints.filter(c => c.tableId == tableId && c.foreignKeyName == Some(foreignKeyName))

  private[datadict] def addConstraint(cstr: schema.Constraint) = {
    implicit val d = this
    constraints += cstr
  }
}