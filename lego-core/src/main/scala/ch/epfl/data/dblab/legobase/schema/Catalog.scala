package ch.epfl.data
package dblab.legobase
package schema

import collection.mutable.{ HashMap, MultiMap, ArrayBuffer }
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
  private[schema] val tablesFromNames = new HashMap[(String, String), TablesRecord]
  private[schema] val tablesFromId = new HashMap[Int, TablesRecord]
  private[schema] val attributesFromTableId = new HashMap[Int, ArrayBuffer[AttributesRecord]]
  private[schema] val rowsFromTableId = new HashMap[Int, ArrayBuffer[RowsRecord]]
  private[schema] val fieldsFromTableAttrRowId = new HashMap[(Int, Int, Int), Any]
  private[schema] val constraintsFromTableId = new HashMap[Int, ArrayBuffer[ConstraintsRecord]]
  private[schema] val sequencesFromName = new HashMap[String, SequencesRecord]

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
    sequencesFromName += constructSequenceName(DDSchemaName, "DD_SEQUENCES", "SEQUENCE_ID") -> SequencesRecord(1, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_SEQUENCES", "SEQUENCE_ID"), this, Some(0))
    sequencesFromName += constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID") -> SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"), this)
    sequencesFromName += constructSequenceName(DDSchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID") -> SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID"), this)
    sequencesFromName += constructSequenceName(DDSchemaName, "DD_ROWS", "ROW_ID") -> SequencesRecord(0, Int.MaxValue, 1, constructSequenceName(DDSchemaName, "DD_ROWS", "ROW_ID"), this)
    val initialSequences = sequencesFromName.map(_._2)

    /* Fill DD_TABLES */
    (0 until dataDictionary.size) foreach { i =>
      val tbl = dd(i)
      val newTablesRecord = TablesRecord(DDSchemaName, tbl.name, this)
      tablesFromId += newTablesRecord.tableId -> newTablesRecord
      tablesFromNames += (newTablesRecord.schemaName, newTablesRecord.name) -> newTablesRecord

      /* For each table in the DD, add a row in DD_TABLES */
      val tableRow = RowsRecord(0, this)
      rowsFromTableId getOrElseUpdate (tableRow.tableId, ArrayBuffer()) += tableRow

      /* Insert values for all rows in DD_TABLES */
      fieldsFromTableAttrRowId += (0, 0, tableRow.rowId) -> tablesFromId(i).schemaName
      fieldsFromTableAttrRowId += (0, 1, tableRow.rowId) -> tablesFromId(i).name
      fieldsFromTableAttrRowId += (0, 2, tableRow.rowId) -> tablesFromId(i).fileName
      fieldsFromTableAttrRowId += (0, 3, tableRow.rowId) -> tablesFromId(i).tableId
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = tablesFromId(i)

      tbl.attributes foreach { attr =>
        /* Insert attributes for all tables */
        val newAttribute = AttributesRecord(i, attr.name, attr.dataType, this)
        attributesFromTableId getOrElseUpdate (tblRecord.tableId, ArrayBuffer()) += newAttribute
        /* Insert rows for DD_ATTRIBUTES */
        val attributesRow = RowsRecord(1, this)
        rowsFromTableId getOrElseUpdate (attributesRow.tableId, ArrayBuffer()) += attributesRow
        /* Insert records for each attribute into DD_FIELDS */
        //TODO Suspected bug
        fieldsFromTableAttrRowId += (1, newAttribute.attributeId, attributesRow.rowId) -> newAttribute.tableId
        fieldsFromTableAttrRowId += (1, newAttribute.attributeId, attributesRow.rowId) -> newAttribute.name
        fieldsFromTableAttrRowId += (1, newAttribute.attributeId, attributesRow.rowId) -> newAttribute.dataType
        fieldsFromTableAttrRowId += (1, newAttribute.attributeId, attributesRow.rowId) -> newAttribute.attributeId
      }
    }

    (0 until dd.size) foreach { i =>
      val tbl = dd(i)
      val tblRecord = tablesFromId(i)

      val constraints = tbl.constraints.filter(filterNotAutoIncrement)
      (0 until constraints.size) foreach { j =>
        /* Insert constraints for all tables */
        val newConstraint = constraints(j).toConstraintsRecord(this, tblRecord.tableId)
        constraintsFromTableId getOrElseUpdate (tblRecord.tableId, ArrayBuffer()) += newConstraint
        /* Insert rows for DD_CONSTRAINTS */
        val constraintRow = RowsRecord(4, this)
        rowsFromTableId getOrElseUpdate (constraintRow.tableId, ArrayBuffer()) += constraintRow

        /* Insert records for each constraint into DD_FIELDS */
        val constraintAttributes = attributesFromTableId(4)
        val tableIdAttrId = constraintAttributes.find(a => a.name == "TABLE_ID").get.attributeId
        fieldsFromTableAttrRowId += (4, tableIdAttrId, constraintRow.rowId) -> newConstraint.tableId
        val cstrTypeAttrId = constraintAttributes.find(a => a.name == "CONSTRAINT_TYPE").get.attributeId
        fieldsFromTableAttrRowId += (4, cstrTypeAttrId, constraintRow.rowId) -> newConstraint.constraintType
        val attrAttrId = constraintAttributes.find(a => a.name == "ATTRIBUTES").get.attributeId
        fieldsFromTableAttrRowId += (4, attrAttrId, constraintRow.rowId) -> newConstraint.attributes
        val refTableAttrId = constraintAttributes.find(a => a.name == "REF_TABLE_NAME").get.attributeId
        fieldsFromTableAttrRowId += (4, refTableAttrId, constraintRow.rowId) -> newConstraint.refTableName
        val refAttrAttrId = constraintAttributes.find(a => a.name == "REF_ATTRIBUTES").get.attributeId
        fieldsFromTableAttrRowId += (4, refAttrAttrId, constraintRow.rowId) -> newConstraint.refAttributes
      }
    }

    initialSequences foreach { seq =>
      /* Insert rows for DD_SEQUENCES */
      val sequenceRow = RowsRecord(5, this)
      rowsFromTableId getOrElseUpdate (sequenceRow.tableId, ArrayBuffer()) += sequenceRow

      /* Insert records for each sequence into DD_FIELDS */
      val sequenceAttributes = attributesFromTableId(5)
      val startValAttrId = sequenceAttributes.find(a => a.name == "START_VALUE").get.attributeId
      fieldsFromTableAttrRowId += (5, startValAttrId, sequenceRow.rowId) -> seq.startValue
      val endValAttrId = sequenceAttributes.find(a => a.name == "END_VALUE").get.attributeId
      fieldsFromTableAttrRowId += (5, endValAttrId, sequenceRow.rowId) -> seq.endValue
      val incrByAttrId = sequenceAttributes.find(a => a.name == "INCREMENT_BY").get.attributeId
      fieldsFromTableAttrRowId += (5, incrByAttrId, sequenceRow.rowId) -> seq.incrementBy
      val seqNameAttrId = sequenceAttributes.find(a => a.name == "NAME").get.attributeId
      fieldsFromTableAttrRowId += (5, seqNameAttrId, sequenceRow.rowId) -> seq.sequenceName
      val seqIdAttrId = sequenceAttributes.find(a => a.name == "SEQUENCE_ID").get.attributeId
      fieldsFromTableAttrRowId += (5, seqIdAttrId, sequenceRow.rowId) -> seq.sequenceId
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
    val newTablesRecord = TablesRecord(schemaName, tbl.name, this, Some(tbl.fileName), Some(newTableId))
    tablesFromId += newTableId -> newTablesRecord
    tablesFromNames += (schemaName, tbl.name) -> newTablesRecord
    addTuple(DDSchemaName, "DD_TABLES", Seq(schemaName, tbl.name, newTableId))

    /* Add entries to DD_ATTRIBUTES */
    implicit def attrIdOrdering: Ordering[AttributesRecord] = Ordering.fromLessThan(_.attributeId < _.attributeId)
    val newAttributes = new ArrayBuffer[AttributesRecord]
    for (attr: Attribute <- tbl.attributes)
      newAttributes += AttributesRecord(newTableId, attr.name, attr.dataType, this)
    attributesFromTableId += newTableId -> newAttributes
    newAttributes.foreach(attr => addTuple(DDSchemaName, "DD_ATTRIBUTES", Seq(newTableId, attr.name, attr.dataType, attr.attributeId)))

    /* Add entries to DD_CONSTRAINTS */
    val newConstraints = new ArrayBuffer[ConstraintsRecord]
    for (cstr <- tbl.constraints.filter(filterNotAutoIncrement))
      newConstraints += cstr.toConstraintsRecord(this, newTableId)
    constraintsFromTableId += newTableId -> newConstraints
    newConstraints.foreach(cstr => addTuple(DDSchemaName, "DD_CONSTRAINTS", Seq(newTableId, cstr.constraintType, cstr.attributes, cstr.refTableName, cstr.refAttributes)))

    /* Add entries to DD_SEQUENCES */
    val newSequences = new ArrayBuffer[SequencesRecord]
    for (cstr <- tbl.constraints.filter(filterAutoIncrement))
      newSequences += {
        cstr match {
          case ai: AutoIncrement => {
            val seqName = constructSequenceName(DDSchemaName, tbl.name, ai.attribute.name)
            val seqRec = SequencesRecord(0, Int.MaxValue, 1, seqName, this)
            sequencesFromName += seqName -> seqRec
            seqRec
          }
          case _ => throw new ClassCastException
        }

      }
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
    val tableId = tablesFromNames(schemaName -> tableName).tableId
    val attributeIds = getAttributes(tableId).map(_.attributeId)
    addTuple(tableId, attributeIds.toList zip values)
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
    rowsFromTableId getOrElseUpdate (tableId, ArrayBuffer()) += row

    for ((attrId, value) <- values)
      fieldsFromTableAttrRowId += (tableId, attrId, row.rowId) -> value
  }

  /** Returns the table with specified name in the given schema */
  def getTable(schemaName: String, tableName: String): TablesRecord = tablesFromNames(schemaName, tableName)

  /** Returns the table with the specified tableId */
  def getTable(tableId: Int) = tablesFromId(tableId)

  /**
   * Returns all tuples for the requested table
   *
   * @param schemaName The name of the schema where this table resides
   * @param tableName The name of the table
   * @return An array of all records for this table
   */
  def getTuples(schemaName: String, tableName: String): Array[Record] =
    getTuples(tablesFromNames(schemaName -> tableName))

  /**
   * Returns all tuples for the requested table
   *
   * @param tableId The id of the table to get the tuples for
   * @return An array of all records for this table
   */
  def getTuples(tableId: Int): Array[Record] =
    getTuples(tablesFromId(tableId))

  /**
   * Returns all tuples for the requested table
   *
   * @param table The table to get the tuples for
   * @return An array of all records for this table
   */
  private def getTuples(table: TablesRecord): Array[Record] = {
    if (!isDataDictionary(table)) /* Only load tables from disk that are not part of the data dictionary */
      Loader.loadTable(this, table)
    rowsFromTableId(table.tableId).map(row => CachingRecord(this, table.tableId, row.rowId)).toArray
  }

  /** Returns the attribute with the specified name in the given table */
  def getAttribute(tableId: Int, attributeName: String) =
    attributesFromTableId(tableId).find(at => at.name == attributeName) match {
      case Some(x) => x
      case None    => throw new Exception(s"Couldn't find attribute $attributeName in table ${tablesFromId(tableId).name}")
    }

  /** Returns all attributes of the specified table */
  def getAttributes(tableId: Int) = attributesFromTableId(tableId)

  /** Returns a list of attributes with the specified names that belong to the given table */
  def getAttributes(tableId: Int, attributeNames: List[String]) = {
    val allTableAttributes = attributesFromTableId(tableId)
    val filteredAttributes = allTableAttributes.filter(a => attributeNames.contains(a.name))

    if (filteredAttributes.size != attributeNames.size)
      throw new Exception(s"Couldn't find all requested attributes in $tableId")
    filteredAttributes
  }

  /** Returns the field identified by the given IDs */
  private[schema] def getField(tableId: Int, attributeId: Int, rowId: Int): Option[Any] = {
    val key = (tableId, attributeId, rowId)
    if (fieldsFromTableAttrRowId contains key)
      Some(fieldsFromTableAttrRowId(key))
    else
      None
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
  private[schema] def getSequenceNext(sequenceName: String): Int =
    sequencesFromName(sequenceName).nextVal

  /** Returns whether a table with the given name already exists in the given schema */
  private def tableExistsAlreadyInDD(schemaName: String, tableName: String): Boolean =
    tablesFromNames contains (schemaName -> tableName)

  /** Indicates whether a table belongs to the data dictionary */
  private def isDataDictionary(table: TablesRecord): Boolean = table.schemaName == DDSchemaName
}