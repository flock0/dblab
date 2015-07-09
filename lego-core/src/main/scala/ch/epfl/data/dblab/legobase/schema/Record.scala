package ch.epfl.data
package dblab.legobase
package schema

import scala.language.dynamics
import collection.mutable.Map

/**
 * A single record in the database
 *
 * @param catalog The catalog that this record is stored in
 * @param tableId The id of the table that describes this records schema
 * @param rowId The id of the row that this record represents
 */
case class Record(private val catalog: Catalog, private val tableId: Int, private val rowId: Int, private val attributeIds: Map[String, Int]) extends sc.pardis.shallow.Record with Dynamic {
  private val tableRecord = catalog.getTable(tableId)
  val schema = tableRecord.schemaName
  val table = tableRecord.name

  def selectDynamic[T](name: String): T = getField(name).get.asInstanceOf[T]

  def getField(key: String): Option[Any] =
    if (attributeIds contains key)
      Some(catalog.getField(tableId, attributeIds(key), rowId))
    else
      None
}
