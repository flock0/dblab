package ch.epfl.data
package dblab.legobase
package schema.datadict

import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import org.scalatest._

class DataDictionaryTest extends FlatSpec {
  "DataDictionary" should "initialize successfully" in {
    val dd = DataDictionary()
    /* Check the size of a few of the DD relations */
    assert(dd.tables.size == 6)
    assert(dd.attributes.size == 25)
    assert(dd.sequences.size == 4)
    /* Check the number of attributes of CONSTRAINTS */
    assert(dd.attributes.filter(a => a.tableId == 4).size == 6)
    /* Check that sequences are working properly */
    assert(dd.sequences(0).nextVal == 4)
    assert(dd.sequences(0).nextVal == 5)
    assert(dd.sequences(1).nextVal == 6)
    /* Check that the values from FIELDS are stored correctly*/
    val rows = dd.rows.filter(r => r.tableId == 0)
    val attributes = dd.attributes.filter(a => a.tableId == 0)
    val tuple = dd.fields.filter(f => f.tableId == 0
      && f.rowId == dd.rows.filter(r => r.tableId == 0)(1).rowId) // 2nd record in TABLES
    assert(tuple.find(f => f.attributeId == dd.attributes(0).attributeId).get.value == "DD")

  }

  it should "return DD tables correctly" in {
    val cat = DDCatalog
    val schema = cat.findSchema(DataDictionary.DDSchemaName)
    val tables = schema.findTable("TABLES").load
    assert(tables.size == 6)
    assert(tables(2).SCHEMA_NAME[String] == DataDictionary.DDSchemaName)

    val seqs = schema.findTable("SEQUENCES").load
    assert(seqs.size == 4)
    assert(seqs(1).START_VALUE[Int] == 0)
    assert(seqs(1).INCREMENT_BY[Int] == 1)
  }
}
