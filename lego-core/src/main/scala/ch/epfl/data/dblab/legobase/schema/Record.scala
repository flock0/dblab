package ch.epfl.data
package dblab.legobase
package schema

import scala.language.dynamics

/**
 * A single record in the database
 *
 * @param catalog The catalog that this record is stored in
 * @param tableId The id of the table that describes this records schema
 * @param rowId The id of the row that this record represents
 */
case class Record(private val catalog: Catalog, private val tableId: Int, private val rowId: Int) extends sc.pardis.shallow.Record with Dynamic {
  private val tableRecord = catalog.getTable(tableId)
  val schema = tableRecord.schemaName
  val table = tableRecord.name

  def selectDynamic[T](name: String): T =
    catalog.getField[T](tableId, catalog.getAttribute(tableId, name).attributeId, rowId)

  def getField(key: String): Option[Any] = Some(selectDynamic[Any](key))
}
