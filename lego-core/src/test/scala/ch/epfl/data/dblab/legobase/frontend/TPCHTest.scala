import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LegoInterpreter
import java.io.PrintStream
import org.scalatest._

class TPCHTest extends FlatSpec {

  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  val SF = System.getenv("LEGO_SF")

  def getOutputName(i: Int) = "TPCH_Q" + i + "_TestResult.txt"

  if (DATAPATH != null && SF != null) {
    info(s"Executing tests for TPC-H with SF$SF")
    "LegoInterpreter" should "execute Q1 without errors" in {
      Console.withOut(new PrintStream(getOutputName(1))) {
        LegoInterpreter.main(Array(DATAPATH, "TPCH", SF, "Q1"))
      }
    }

    for (i <- 2 to 99) {
      it should s"execute Q$i without errors" in {
        Console.withOut(new PrintStream(getOutputName(i))) {
          LegoInterpreter.main(Array(DATAPATH, "TPCH", SF, "Q" + i))
        }
      }
    }
  } else {
    info("Tests could not run because the environment variable `LEGO_DATA_FOLDER` or `LEGO_SF` does not exist.")
  }

}