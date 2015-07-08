package ch.epfl.data
package dblab.legobase
package schema

import Catalog._
import sc.pardis.types._

/* Case classes for the tables in the data dictionary */
case class TablesRecord(schemaName: String, name: String, private val catalog: Catalog, val fileName: Option[String] = None, private val _tableId: Option[Int] = None, var isLoaded: Boolean = false) {
  val tableId = _tableId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_TABLES", "TABLE_ID"))
  }
}
case class AttributesRecord(tableId: Int, name: String, dataType: Tpe, private val catalog: Catalog, private val _attributeId: Option[Int] = None) {
  val attributeId = _attributeId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_ATTRIBUTES", "ATTRIBUTE_ID"))
  }
}
case class FieldsRecord(tableId: Int, attributeId: Int, rowId: Int, value: Any)
case class RowsRecord(tableId: Int, private val catalog: Catalog, private val _rowId: Option[Int] = None) {
  val rowId = _rowId match {
    case Some(id) => id
    case None     => catalog.getSequenceNext(constructSequenceName(DDSchemaName, "DD_ROWS", "ROW_ID"))
  }
}
case class ConstraintsRecord(tableId: Int, constraintType: Char, attributes: List[Int], refTableName: Option[String], refAttributes: Option[List[String]])
case class SequencesRecord(startValue: Int, endValue: Int, incrementBy: Int, sequenceName: String, private val catalog: Catalog, private val _sequenceId: Option[Int] = None) {
  /* Catch invalid start/end/incrementBy values*/
  if (incrementBy == 0)
    throw new Exception(s"incrementBy of $sequenceName must not be 0")
  if (startValue < endValue) {
    if (incrementBy < 0) {
      throw new Exception(s"Sequence $sequenceName can never reach the end value")
    }
  } else {
    if (incrementBy > 0) {
      throw new Exception(s"Sequence $sequenceName can never reach the end value")

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
      throw new Exception(s"Sequence $sequenceName has reached it's maximum value $endValue")
    val value = next
    next += incrementBy
    //TODO Update field value in DD_FIELD for this sequence
    value
  }
}