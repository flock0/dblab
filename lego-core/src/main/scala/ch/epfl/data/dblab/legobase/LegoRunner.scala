package ch.epfl.data
package dblab.legobase

import tpch._
import schema._
import frontend._
import frontend.optimizer._
import utils.Utilities._
import java.io.PrintStream

/**
 * The common trait for all Query Runners (either a Query Interpreter or a Query Compiler)
 */
trait LegoRunner {
  var currQuery: java.lang.String = ""
  Config.checkResults = true

  def getOutputName = currQuery + "Output.txt"

  /**
   * Executes the given TPCH query with the given scaling factor.
   *
   * This method should be implemented by a query interpreter to interpret the given
   * query or by a query compiler to compile the given query.
   *
   * @param query the input TPCH query (TODO should be generalized)
   */
  def executeQuery(query: String, schema: Schema): Unit

  /**
   * The starting point of a query runner which uses the arguments as its setting.
   *
   * @param args the setting arguments passed through command line
   */
  def run(args: Array[String]) {

    val sf = if (args(1).contains(".")) args(1).toDouble.toString else args(1).toInt.toString
    Config.sf = sf.toDouble
    Config.datapath = args(0) + "/sf" + sf + "/"

    val excludedQueries = Nil

    val ddlDefStr = scala.io.Source.fromFile("tpch/dss.ddl").mkString
    val schemaDDL = DDLParser.parse(ddlDefStr)
    val constraintsDefStr = scala.io.Source.fromFile("tpch/dss.ri").mkString
    val constraintsDDL = DDLParser.parse(constraintsDefStr)
    val schemaWithConstraints = schemaDDL ++ constraintsDDL
    val schema = DDLInterpreter.interpret(schemaWithConstraints)
    System.out.println(schema)

    //val dropDefStr = scala.io.Source.fromFile("/home/klonatos/Work/dblab/tpch/drop.sql").mkString
    //val dropDDL = DDLParser.parse(dropDefStr)
    //DDLInterpreter.interpret(dropDDL)
    //System.out.println(finalSchema)

    // TODO: These stats will die soon
    schema.stats += "DISTINCT_L_SHIPMODE" -> 7
    schema.stats += "DISTINCT_L_RETURNFLAG" -> 3
    schema.stats += "DISTINCT_L_LINESTATUS" -> 2
    schema.stats += "DISTINCT_L_ORDERKEY" -> schema.stats.getCardinality("LINEITEM")
    schema.stats += "DISTINCT_L_PARTKEY" -> schema.stats.getCardinality("PART")
    schema.stats += "DISTINCT_L_SUPPKEY" -> schema.stats.getCardinality("SUPPLIER")
    schema.stats += "DISTINCT_N_NAME" -> 25
    schema.stats += "DISTINCT_O_SHIPPRIORITY" -> 1
    schema.stats += "DISTINCT_O_ORDERDATE" -> 365 * 7 // 7-whole years
    schema.stats += "DISTINCT_O_ORDERPRIORITY" -> 5
    schema.stats += "DISTINCT_O_ORDERKEY" -> schema.stats.getCardinality("LINEITEM")
    schema.stats += "DISTINCT_O_CUSTKEY" -> schema.stats.getCardinality("CUSTOMER")
    schema.stats += "DISTINCT_P_PARTKEY" -> schema.stats.getCardinality("PART")
    schema.stats += "DISTINCT_P_BRAND" -> 25
    schema.stats += "DISTINCT_P_SIZE" -> 50
    schema.stats += "DISTINCT_P_TYPE" -> 150
    schema.stats += "DISTINCT_PS_PARTKEY" -> schema.stats.getCardinality("PART")
    schema.stats += "DISTINCT_PS_SUPPKEY" -> schema.stats.getCardinality("SUPPLIER")
    schema.stats += "DISTINCT_PS_AVAILQTY" -> 9999
    schema.stats += "DISTINCT_S_NAME" -> schema.stats.getCardinality("SUPPLIER")
    schema.stats += "DISTINCT_S_NATIONKEY" -> 25
    schema.stats += "DISTINCT_C_CUSTKEY" -> schema.stats.getCardinality("CUSTOMER")
    schema.stats += "DISTINCT_C_NAME" -> schema.stats.getCardinality("CUSTOMER")
    schema.stats += "DISTINCT_C_NATIONKEY" -> 25

    schema.stats += "NUM_YEARS_ALL_DATES" -> 7

    System.out.println(schema.stats.mkString("\n"))

    val queries: scala.collection.immutable.List[String] =
      if (args.length >= 3 && args(2) == "testsuite-scala") (for (i <- 1 to 22 if !excludedQueries.contains(i)) yield "Q" + i).toList
      else if (args.length >= 3 && args(2) == "testsuite-c") (for (i <- 1 to 22 if !excludedQueries.contains(i)) yield "Q" + i + "_C").toList
      else args.drop(2).filter(x => !x.startsWith("+") && !x.startsWith("-")).toList
    for (q <- queries) {
      currQuery = q
      Console.withOut(new PrintStream(getOutputName)) {
        //executeQuery(currQuery, schema)
        val qStmt = SQLParser.parse(scala.io.Source.fromFile("tpch/" + currQuery + ".sql").mkString)
        System.out.println(qStmt + "\n\n")
        new SQLSemanticCheckerAndTypeInference(schema).checkAndInfer(qStmt)
        val operatorTree = new SQLTreeToOperatorTreeConverter(schema).convert(qStmt)
        val optimizerTree = if (q != "Q19" && q != "Q16" && q != "Q22") new NaiveOptimizer(schema).optimize(operatorTree) else operatorTree // TODO -- FIX OPTIMIZER FOR Q19
        //System.out.println(optimizezr.registeredPushedUpSelections.map({ case (k, v) => (k.name, v) }).mkString(","))
        System.out.println(optimizerTree + "\n\n")

        val qp = new SQLTreeToQueryPlanConverter(schema).convert(optimizerTree)

        // Check results
        if (Config.checkResults) {
          val getResultFileName = "results/" + currQuery + ".result_sf" + sf
          val resq = scala.io.Source.fromFile(getOutputName).mkString
          if (new java.io.File(getResultFileName).exists) {
            val resc = {
              val str = scala.io.Source.fromFile(getResultFileName).mkString
              str * Config.numRuns
            }
            if (resq != resc) {
              System.out.println("-----------------------------------------")
              System.out.println("QUERY" + q + " DID NOT RETURN CORRECT RESULT!!!")
              System.out.println("Correct result:")
              System.out.println(resc)
              System.out.println("Result obtained from execution:")
              System.out.println(resq)
              System.out.println("-----------------------------------------")
              System.exit(0)
            } else System.out.println("CHECK RESULT FOR QUERY " + q + ": [OK]")
          } else {
            System.out.println("Reference result file not found. Skipping checking of result")
            System.out.println("Execution results:")
            System.out.println(resq)
          }
        }
      }
    }
  }
}
