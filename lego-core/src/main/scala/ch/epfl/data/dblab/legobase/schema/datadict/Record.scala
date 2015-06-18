package ch.epfl.data
package dblab.legobase
package schema.datadict

import scala.language.dynamics

/**
 * A single record in the database
 *
 * @param dict The data dictionary that this record is stored in
 * @param tableId The id of the table that describes this records schema
 * @param rowId The id of the row that this record represents
 */
case class Record(private val dict: DataDictionary, private val tableId: Int, private val rowId: Int) extends sc.pardis.shallow.Record with Dynamic {
  private val tableRecord = dict.getTable(tableId)
  val schema = tableRecord.schemaName
  val table = tableRecord.name

  def selectDynamic[T](name: String): T = {
    if (!dict.rowExists(tableId, rowId))
      throw new Exception(s"Row $rowId doesn't exist in $schema.$table")
    dict.getField[T](tableId, dict.getAttribute(tableId, name).attributeId, rowId) match {
      case Some(v) => v
      case None    => throw new Exception(s"Field value for $name doesn't exist in row $rowId of $schema.$table")
    }
  }

  def getField(key: String): Option[Any] = ???
}
