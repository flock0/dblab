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
  private val cache: Map[String, Any] = Map()

  override def selectDynamic[T](name: String): T =
    if (cache.contains(name)) {
      cache(name).asInstanceOf[T]
    } else {
      val field = super.selectDynamic[T](name)
      cache += name -> field
      field
    }

}

object CachingRecord {
  def apply(catalog: Catalog, tableId: Int, rowId: Int) = new CachingRecord(catalog, tableId, rowId)
}