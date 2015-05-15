import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import ch.epfl.data.dblab.legobase.tpch.TPCHSchema
import ch.epfl.data.dblab.legobase.schema.Catalog
import org.scalatest._

class SchemaTest extends FlatSpec {
  "Catalog" should "initialize successfully" in {
    val cat = Catalog(Map())
    /* Check the size of a few of the DD relations */
    assert(cat.ddTables.size == 6)
    assert(cat.ddAttributes.size == 24)
    assert(cat.ddSequences.size == 4)
    /* Check the number of attributes of DD_CONSTRAINTS */
    assert(cat.ddAttributes.filter(a => a.tableId == 4).size == 5)
    /* Check that sequences are working properly */
    assert(cat.ddSequences(0).nextVal == 4)
    assert(cat.ddSequences(0).nextVal == 5)
    assert(cat.ddSequences(1).nextVal == 6)
    /* Check that the values from DD_FIELDS are stored correctly*/
    val rows = cat.ddRows.filter(r => r.tableId == 0)
    val attributes = cat.ddAttributes.filter(a => a.tableId == 0)
    val tuple = cat.ddFields.filter(f => f.tableId == 0
      && f.rowId == cat.ddRows.filter(r => r.tableId == 0)(1).rowId) // 2nd record in DD_TABLES
    assert(tuple.find(f => f.attributeId == cat.ddAttributes(0).attributeId).get.value == "DD")

    //TODO Write more extensive tests
  }
  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  if (DATAPATH != null) {
    "Catalog" should "load TPCH schema correctly" in {
      val cat = Catalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
      fail("Not tested yet") //TODO Write tests
    }
  } else {
    fail("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }
}
