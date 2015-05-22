package ch.epfl.data
package dblab.legobase
package schema

import scala.language.dynamics

case class Record(private val catalog: Catalog, private val tableId: Int, private val rowId: Int) extends Dynamic {
  private val tableRecord = catalog.getTableRecord(tableId)
  val schema = tableRecord.schemaName
  val table = tableRecord.name

  def selectDynamic[T](name: String): T = {
    if (!catalog.rowExists(tableId, rowId))
      throw new Exception(s"Row $rowId doesn't exist in $schema.$table")
    catalog.getAttribute(tableId, name) match {
      case Some(at) => catalog.getField[T](tableId, at.attributeId, rowId) match {
        case Some(v) => v
        case None    => throw new Exception(s"Field value for $name doesn't exist in row $rowId of $schema.$table")
      }
      case None => throw new Exception(s"Attribute $name doesn't exist in table $schema.$table")
    }
  }
}