package ch.epfl.data
package dblab.legobase

import sc.pardis.shallow.{ Record, OptimalString }

case class LegobaseRecord(values: Seq[(String, Any)]) extends Record {
  private val fieldMap = collection.mutable.HashMap[String, Any](values: _*)
  def numFields = fieldMap.size
  def getField(name: String): Option[Any] = fieldMap.get(name) match {
    case Some(fld) => Some(fld)
    case None      => None
  }
  def setField(name: String, value: Any) = fieldMap(name) = value
  def getFieldNames() = fieldMap.keySet
  def getNestedRecords(): Seq[LegobaseRecord] = fieldMap.map({ case (k, v) => v }).filter(_.isInstanceOf[LegobaseRecord]).toSeq.asInstanceOf[Seq[LegobaseRecord]]
}
