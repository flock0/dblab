package ch.epfl.data
package dblab.legobase
package schema.datadict

import scala.language.dynamics

/**
 * A single tuple in the database
 *
 * @param dict The data dictionary that this tuple is stored in
 * @param tableId The id of the table that describes this tuples schema
 * @param rowId The id of the row that this tuple represents
 */
case class Tuple(private val dict: DataDictionary, private val tableId: Int, private val rowId: Int) extends schema.DynamicRecord {
  private val tableRecord = dict.getTable(tableId)
  val schema = tableRecord.schemaName
  val table = tableRecord.name

  override def selectDynamic[T](name: String): T = getField(name) match {
    case Some(v) => v.asInstanceOf[T]
    case None => throw new Exception(s"Field value for $name doesn't exist in row $rowId of $schema.$table" +
      s"OR Row $rowId doesn't exist in $schema.$table")
  }

  def getField(name: String): Option[Any] = {
    if (!dict.rowExists(tableId, rowId))
      None
    else
      dict.getField[Any](tableId, dict.getAttribute(tableId, name).attributeId, rowId)
  }
}
