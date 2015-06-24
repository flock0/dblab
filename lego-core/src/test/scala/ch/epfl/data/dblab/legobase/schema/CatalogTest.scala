package ch.epfl.data
package dblab.legobase
package schema


import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import org.scalatest._ 
class CatalogTest extends FlatSpec with BeforeAndAfterEach{
  
  override def beforeEach() {
    addTPCHSchema(cat)
  }

   // Close and delete the temp file
   override def afterEach() {
     val schema = cat.findSchema("TPCH")
     val tables = schema.tables
     tables.foreach(schema.dropTable(_.name))
   }


  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  if (DATAPATH != null) {
    val ddCat = DDCatalog
    executeTests(DDCatalog, "DDCatalog")
    executeTests(MapCatalog, "MapCatalog")
  } else {
    fail("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }

  def executeTests(cat: Catalog, catName: String) = {
    catName should "add TPCH schema correctly" in {
      // TPCH schema is added in the beforeEach method
      val schema = cat.findSchema("TPCH")
      val tables = schema.tables
      assert(tables.size == 8)

      val tableNames = tables.map(_.name)
      assert(tableNames.fold(true)
        (_ && Seq("LINEITEM", "ORDERS", "CUSTOMER", "SUPPLIER", 
                  "PARTSUPP", "REGION", "NATION", "PART") contains _))
      
      //TODO test attributes of one of the tables
    }

    it should "drop tables correctly" in {
      val schema = cat.findSchema("TPCH")
      val tables = schema.tables
      tables.foreach(schema.dropTable(_.name))
      assert(cat.findSchema("TPCH").schema.tables.size == 0)
    }

    it should "load PART only once" in {
      //TODO
      // val cat = CurrCatalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
      // val records = cat.getTuples("TPCH", "PART")
      // assert(records.size == 20000)
      // assert(records(0).P_PARTKEY[Int] == 1)
      // assert(records(19999).P_RETAILPRICE[Double] == 920.00)
      // val sameRecords = cat.getTuples("TPCH", "PART")
      // assert(sameRecords.size == 20000)
      // assert(sameRecords(0).P_PARTKEY[Int] == 1)
      // assert(sameRecords(19999).P_RETAILPRICE[Double] == 920.00)
    }
  }
}
