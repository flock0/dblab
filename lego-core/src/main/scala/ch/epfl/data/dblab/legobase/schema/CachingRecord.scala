package ch.epfl.data
package dblab.legobase
package schema

import scala.collection.mutable.Map

/**
 * A single record in the database that caches values already fetched
 * to avoid the detour over the Catalog.
 *
 * @param catalog The catalog that this record is stored in
 * @param tableId The id of the table that describes this records schema
 * @param rowId The id of the row that this record represents
 */
class CachingRecord(private val catalog: Catalog, private val tableId: Int, private val rowId: Int) extends Record(catalog, tableId, rowId) {
  private val cache: Map[String, Option[Any]] = Map()

  override def getField(key: String): Option[Any] =
    if (cache.contains(key)) {
      cache(key)
    } else {
      val field = super.getField(key)
      cache += key -> field
      field
    }
}

object CachingRecord {
  def apply(catalog: Catalog, tableId: Int, rowId: Int) = new CachingRecord(catalog, tableId, rowId)
}