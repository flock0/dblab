import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import ch.epfl.data.dblab.legobase.tpch.TPCHSchema
import org.scalatest._

class TPCDSTest extends FlatSpec {

  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  if (DATAPATH != null) {
    info("Executing tests for TPC-DS with SF1")
    "LegoInterpreter" should "execute Q1 without errors" in {
      LegoInterpreter.main(Array(DATAPATH, "TPCDS", "1", "1"))
    }

    for (i <- 2 to 99) {
      it should s"execute Q$i without errors" in {
        LegoInterpreter.main(Array(DATAPATH, "TPCDS", "1", i))
      }
    }
  } else {
    println("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }

}