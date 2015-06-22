package ch.epfl.data
package dblab.legobase
package schema

import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import org.scalatest._

class SchemaTest extends FlatSpec {
  "DDCatalog" should "initialize successfully" in {
    val schema = DDCatalog.findSchema("DD")
    /* Check the size of a few of the DD relations */
    assert(schema.tables.size == 6)
    assert(schema.attributes.size == 24)
    assert(schema.sequences.size == 4)
    /* Check the number of attributes of CONSTRAINTS */
    assert(cat.attributes.filter(a => a.tableId == 4).size == 5)
    /* Check that sequences are working properly */
    assert(cat.sequences(0).nextVal == 4)
    assert(cat.sequences(0).nextVal == 5)
    assert(cat.sequences(1).nextVal == 6)
    /* Check that the values from FIELDS are stored correctly*/
    val rows = cat.rows.filter(r => r.tableId == 0)
    val attributes = cat.attributes.filter(a => a.tableId == 0)
    val tuple = cat.fields.filter(f => f.tableId == 0
      && f.rowId == cat.rows.filter(r => r.tableId == 0)(1).rowId) // 2nd record in TABLES
    assert(tuple.find(f => f.attributeId == cat.attributes(0).attributeId).get.value == "DD")

  }

  it should "return DD tables correctly" in {
    val cat = Catalog(Map())
    val tables = cat.getTuples(Catalog.DDSchemaName, "TABLES")
    assert(tables.size == 6)
    assert(tables(2).SCHEMA_NAME[String] == Catalog.DDSchemaName)

    val seqs = cat.getTuples(Catalog.DDSchemaName, "SEQUENCES")
    assert(seqs.size == 4)
    assert(seqs(1).START_VALUE[Int] == 0)
    assert(seqs(1).INCREMENT_BY[Int] == 1)
  }

  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  if (DATAPATH != null) {
    it should "load PART table correctly" in {
      val cat = Catalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
      Loader.loadTable(cat, "TPCH", "PART")
      val records = cat.getTuples("TPCH", "PART")
      assert(records.size == 20000)
      assert(records(0).P_PARTKEY[Int] == 1)
      assert(records(19999).P_RETAILPRICE[Double] == 920.00)
    }

    it should "load PART table automatically" in {
      val cat = Catalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
      val records = cat.getTuples("TPCH", "PART")
      assert(records.size == 20000)
      assert(records(0).P_PARTKEY[Int] == 1)
      assert(records(19999).P_RETAILPRICE[Double] == 920.00)
    }

    it should "load PART only once" in {
      val cat = Catalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
      val records = cat.getTuples("TPCH", "PART")
      assert(records.size == 20000)
      assert(records(0).P_PARTKEY[Int] == 1)
      assert(records(19999).P_RETAILPRICE[Double] == 920.00)
      val sameRecords = cat.getTuples("TPCH", "PART")
      assert(sameRecords.size == 20000)
      assert(sameRecords(0).P_PARTKEY[Int] == 1)
      assert(sameRecords(19999).P_RETAILPRICE[Double] == 920.00)
    }
  } else {
    fail("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }
}
