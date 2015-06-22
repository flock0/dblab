package ch.epfl.data
package dblab.legobase

import schema._
import utils.Utilities._
import java.io.PrintStream
import tpch.Queries
import tpch.CatalogQueries

/**
 * The main object for interpreting queries.
 */
object LegoInterpreter extends LegoRunner {

  /**
   * Interprets the given TPCH query with the given scaling factor.
   *
   * @param query the input TPCH query (TODO should be generalized)
   */
  def executeQuery(query: String, scalingFactor: Double): Unit = query match {
    case "Q1"     => Queries.Q1(Config.numRuns)
    case "Q2"     => Queries.Q2(Config.numRuns)
    case "Q3"     => Queries.Q3(Config.numRuns)
    case "Q4"     => Queries.Q4(Config.numRuns)
    case "Q5"     => Queries.Q5(Config.numRuns)
    case "Q6"     => Queries.Q6(Config.numRuns)
    case "Q7"     => Queries.Q7(Config.numRuns)
    case "Q8"     => Queries.Q8(Config.numRuns)
    case "Q9"     => Queries.Q9(Config.numRuns)
    case "Q10"    => Queries.Q10(Config.numRuns)
    case "Q11"    => Queries.Q11(Config.numRuns)
    case "Q12"    => Queries.Q12(Config.numRuns)
    case "Q13"    => Queries.Q13(Config.numRuns)
    case "Q14"    => Queries.Q14(Config.numRuns)
    case "Q15"    => Queries.Q15(Config.numRuns)
    case "Q16"    => Queries.Q16(Config.numRuns)
    case "Q17"    => Queries.Q17(Config.numRuns)
    case "Q18"    => Queries.Q18(Config.numRuns)
    case "Q19"    => Queries.Q19(Config.numRuns)
    case "Q20"    => Queries.Q20(Config.numRuns)
    case "Q21"    => Queries.Q21(Config.numRuns)
    case "Q22"    => Queries.Q22(Config.numRuns)
    case "Q1c"    => CatalogQueries.Q1(Config.numRuns)
    case "Q2c"    => CatalogQueries.Q2(Config.numRuns)
    case "Q3c"    => CatalogQueries.Q3(Config.numRuns)
    case "Q4c"    => CatalogQueries.Q4(Config.numRuns)
    case "Q5c"    => CatalogQueries.Q5(Config.numRuns)
    case "Q6c"    => CatalogQueries.Q6(Config.numRuns)
    case "Q7c"    => CatalogQueries.Q7(Config.numRuns)
    case "Q8c"    => CatalogQueries.Q8(Config.numRuns)
    case "Q9c"    => CatalogQueries.Q9(Config.numRuns)
    case "Q10c"   => CatalogQueries.Q10(Config.numRuns)
    case "Q11c"   => CatalogQueries.Q11(Config.numRuns)
    case "Q12c"   => CatalogQueries.Q12(Config.numRuns)
    case "Q13c"   => CatalogQueries.Q13(Config.numRuns)
    case "Q14c"   => CatalogQueries.Q14(Config.numRuns)
    case "Q15c"   => CatalogQueries.Q15(Config.numRuns)
    case "Q16c"   => CatalogQueries.Q16(Config.numRuns)
    case "Q17c"   => CatalogQueries.Q17(Config.numRuns)
    case "Q18c"   => CatalogQueries.Q18(Config.numRuns)
    case "Q19c"   => CatalogQueries.Q19(Config.numRuns)
    case "Q20c"   => CatalogQueries.Q20(Config.numRuns)
    case "Q21c"   => CatalogQueries.Q21(Config.numRuns)
    case "Q22c"   => CatalogQueries.Q22(Config.numRuns)
    case dflt @ _ => throw new Exception("Query " + dflt + " not supported!")
  }

  def main(args: Array[String]) {
    // Some checks to avoid stupid exceptions
    if (args.length < 3) {
      System.out.println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      System.out.println("USAGE: run <data_folder> <scaling_factor_number> <list of queries to run>")
      System.out.println("     : data_folder_name should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.exit(0)
    }

    run(args)
  }
}
