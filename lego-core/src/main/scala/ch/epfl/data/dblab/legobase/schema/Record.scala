package ch.epfl.data
package dblab.legobase
package schema

import scala.language.dynamics

class Record(private val catalog: Catalog, private val tableId: Int, private val rowId: Int) extends Dynamic {
  private val tableRecord = catalog.ddTables.find(tbl => tbl.tableId == tableId) match {
    case Some(t) => t
    case None    => throw new Exception(s"Table with ID $tableId doesn't exist in this catalog")
  }
  val schema = tableRecord.schemaName
  val table = tableRecord.name

  def selectDynamic(name: String) = {
    if (!catalog.rowExists(tableId, rowId))
      throw new Exception(s"Row $rowId doesn't exist in table $tableId")
    catalog.getAttribute(tableId, name) match {
      case Some(at) => catalog.getField(tableId, at.attributeId, rowId)
      case None     => throw new Exception(s"Attribute $name doesn't exist in table $schema.$table")
    }
  }
}