import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LegoInterpreter
import org.scalatest._

class TPCDSTest extends FlatSpec {

  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  val SF = System.getenv("LEGO_SF")
  if (DATAPATH != null && SF != null) {
    info(s"Executing tests for TPC-DS with SF$SF")
    "LegoInterpreter" should "execute Q1 without errors" in {
      LegoInterpreter.main(Array(DATAPATH, "TPCDS", SF, "1"))
    }

    for (i <- 2 to 99) {
      it should s"execute Q$i without errors" in {
        LegoInterpreter.main(Array(DATAPATH, "TPCDS", SF, i.toString))
      }
    }
  } else {
    println("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }

}