package ch.epfl.data
package dblab.legobase
package schema.datadict

import scala.language.implicitConversions
import DataDictionary._
import sc.pardis.types._
import schema.{ Constraint, PrimaryKey, ForeignKey, NotNull, Unique, AutoIncrement, Compressed }

/* Case classes for the tables in the data dictionary */
case class TablesRecord(schemaName: String, name: String, private val dict: DataDictionary, val fileName: Option[String] = None, private val _tableId: Option[Int] = None, var isLoaded: Boolean = false) {
  val tableId = _tableId match {
    case Some(id) => id
    case None     => dict.getSequenceNext(constructSequenceName(DDSchemaName, "TABLES", "TABLE_ID"))
  }
}
case class AttributesRecord(tableId: Int, name: String, dataType: Tpe, private val dict: DataDictionary, private val _attributeId: Option[Int] = None) {
  val attributeId = _attributeId match {
    case Some(id) => id
    case None     => dict.getSequenceNext(constructSequenceName(DDSchemaName, "ATTRIBUTES", "ATTRIBUTE_ID"))
  }
}
case class FieldsRecord(tableId: Int, attributeId: Int, rowId: Int, value: Any)
case class RowsRecord(tableId: Int, private val dict: DataDictionary, private val _rowId: Option[Int] = None) {
  val rowId = _rowId match {
    case Some(id) => id
    case None     => dict.getSequenceNext(constructSequenceName(DDSchemaName, "ROWS", "ROW_ID"))
  }
}
case class ConstraintsRecord(tableId: Int, constraintType: Char, attributes: List[Int], refTableName: Option[String] = None, refAttributes: Option[List[String]] = None, foreignKeyName: Option[String] = None)

case class SequencesRecord(startValue: Int, endValue: Int, incrementBy: Int, sequenceName: String, private val dict: DataDictionary, private val _sequenceId: Option[Int] = None) {
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
    case None     => dict.getSequenceNext(constructSequenceName(DDSchemaName, "SEQUENCES", "SEQUENCE_ID"))
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
    //TODO Update field value in FIELD for this sequence
    value
  }
}

object ConstraintsRecord {
  implicit def ConstraintToConstraintsRecord(cr: Constraint)(implicit dict: DataDictionary, tableId: Int): ConstraintsRecord = cr match {
    case PrimaryKey(attributes) => {
      val attributeIds = dict.getAttributes(tableId, attributes.map(_.name).toList).map(a => a.attributeId)
      ConstraintsRecord(tableId, 'p', attributeIds)
    }
    case ForeignKey(fkName, own, ref, attrs) => ???
    case NotNull(attr) => {
      val attributeId = List(dict.getAttribute(tableId, attr.name).attributeId)
      ConstraintsRecord(tableId, 'n', attributeId)
    }
    case Unique(attr) => {
      val attributeId = List(dict.getAttribute(tableId, attr.name).attributeId)
      ConstraintsRecord(tableId, 'u', attributeId)
    }
    case Compressed => ??? //TODO Compressed is currently not a constraint in the data dictionary
  }
}