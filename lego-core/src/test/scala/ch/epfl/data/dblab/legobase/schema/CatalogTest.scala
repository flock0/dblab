package ch.epfl.data
package dblab.legobase
package schema

import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import org.scalatest._

class CatalogTest extends FlatSpec {
  //TOOD Adapt tests to merged catalogs
  // val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  // if (DATAPATH != null) {
  //   it should "load PART table correctly" in {
  //     val cat = ???CurrCatalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
  //     Loader.loadTable(cat, "TPCH", "PART")
  //     val records = cat.getTuples("TPCH", "PART")
  //     assert(records.size == 20000)
  //     assert(records(0).P_PARTKEY[Int] == 1)
  //     assert(records(19999).P_RETAILPRICE[Double] == 920.00)
  //   }

  //   it should "load PART table automatically" in {
  //     val cat = CurrCatalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
  //     val records = cat.getTuples("TPCH", "PART")
  //     assert(records.size == 20000)
  //     assert(records(0).P_PARTKEY[Int] == 1)
  //     assert(records(19999).P_RETAILPRICE[Double] == 920.00)
  //   }

  //   it should "load PART only once" in {
  //     val cat = CurrCatalog(Map("TPCH" -> TPCHSchema.getSchema(s"$DATAPATH/sf0.1/", 0.1)))
  //     val records = cat.getTuples("TPCH", "PART")
  //     assert(records.size == 20000)
  //     assert(records(0).P_PARTKEY[Int] == 1)
  //     assert(records(19999).P_RETAILPRICE[Double] == 920.00)
  //     val sameRecords = cat.getTuples("TPCH", "PART")
  //     assert(sameRecords.size == 20000)
  //     assert(sameRecords(0).P_PARTKEY[Int] == 1)
  //     assert(sameRecords(19999).P_RETAILPRICE[Double] == 920.00)
  //   }
  // } else {
  //   fail("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  // }
}
