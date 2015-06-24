package ch.epfl.data
package dblab.legobase
package schema.datadict

import scala.collection.mutable.Map

/**
 * A single tuple in the database that caches values already fetched
 * to avoid the detour over the Catalog.
 *
 * @param dict The data dictionary that this tuple is stored in
 * @param tableId The id of the table that describes this tuple schema
 * @param rowId The id of the row that this tuple represents
 */
class CachingTuple(private val dict: DataDictionary, private val tableId: Int, private val rowId: Int) extends Tuple(dict, tableId, rowId) {
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

object CachingTuple {
  def apply(dict: DataDictionary, tableId: Int, rowId: Int) = new CachingTuple(dict, tableId, rowId)
}