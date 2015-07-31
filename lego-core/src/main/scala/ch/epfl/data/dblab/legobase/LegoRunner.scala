package ch.epfl.data
package dblab.legobase

//import tpch._
import schema._
import frontend._
import frontend.optimizer._
import frontend.normalizer.EquiJoinNormalizer
import benchmarks._
import utils.Utilities._
import java.io.PrintStream
import ch.epfl.data.dblab.legobase.frontend.OperatorAST._

/**
 * The common trait for all Query Runners (either a Query Interpreter or a Query Compiler)
 */
trait LegoRunner {
  var currQuery: java.lang.String = ""

  def getOutputName = currQuery + "Output.txt"

  /*
   * This method should be implemented by a query interpreter to interpret the given
   * query or by a query compiler to compile the given query.
   *
   * @param queryPlan -- the query plan to be executed
   */
  def executeQuery(queryPlan: OperatorNode, schema: Schema): Unit

  /**
   * The starting point of a query runner which uses the arguments as its setting.
   *
   * @param args the setting arguments passed through command line
   */
  def run(args: Array[String]) {

    val sf = if (args(2).contains(".")) args(2).toDouble.toString else args(2).toInt.toString
    Config.sf = sf.toDouble
    Config.datapath = args(0) + "/sf" + sf + "/"

    val excludedQueries = Nil

    val queryConf: BenchConfig =
      if (args(1) == "TPCH")
        TPCHConfig
      else if (args(1) == "TPCDS")
        TPCDSConfig
      else
        throw new Exception("Invalid test suite!")

    Config.checkResults = queryConf.checkResult

    val ddlDefStr = scala.io.Source.fromFile(queryConf.ddlPath).mkString
    val schemaDDL = DDLParser.parse(ddlDefStr)
    val constraintsDefStr = scala.io.Source.fromFile(queryConf.constraintsPath).mkString
    val constraintsDDL = DDLParser.parse(constraintsDefStr)
    val schemaWithConstraints = schemaDDL ++ constraintsDDL
    val schema = DDLInterpreter.interpret(schemaWithConstraints)
    println(schema)

    //val dropDefStr = scala.io.Source.fromFile("/home/klonatos/Work/dblab/tpch/drop.sql").mkString
    //val dropDDL = DDLParser.parse(dropDefStr)
    //DDLInterpreter.interpret(dropDDL)
    //System.out.println(finalSchema)

    queryConf.addStats(schema)

    println(schema.stats.mkString("\n"))
    val ejNorm = new EquiJoinNormalizer(schema)
    val queries: scala.collection.immutable.List[String] =
      if (args.length >= 4 && args(3) == "testsuite-scala") (for (i <- 1 to 22 if !excludedQueries.contains(i)) yield "Q" + i).toList
      else if (args.length >= 4 && args(3) == "testsuite-scala-ds") (for (i <- 1 to 99 if !excludedQueries.contains(i)) yield "Q" + i).toList
      else if (args.length >= 4 && args(3) == "testsuite-c") (for (i <- 1 to 22 if !excludedQueries.contains(i)) yield "Q" + i + "_C").toList
      else args.drop(3).filter(x => !x.startsWith("+") && !x.startsWith("-")).toList
    for (q <- queries) {
      currQuery = q
      Console.withOut(new PrintStream(getOutputName)) {
        //executeQuery(currQuery, schema)
        val origStmt = SQLParser.parse(scala.io.Source.fromFile(queryConf.queryFolder + currQuery + ".sql").mkString)

        val qStmt = ejNorm.normalize(origStmt)
        println("====== BEFORE NORMALIZATION ======\n" + origStmt + "\n")
        println("====== AFTER NORMALIZATION ======\n" + qStmt + "\n")
        new SQLSemanticCheckerAndTypeInference(schema).checkAndInfer(qStmt)
        val operatorTree = new SQLTreeToOperatorTreeConverter(schema).convert(qStmt)
        val optimizerTree = if ((args(1) == "TPCH") && (q != "Q19" && q != "Q16" && q != "Q22")) new NaiveOptimizer(schema).optimize(operatorTree) else operatorTree // TODO -- FIX OPTIMIZER FOR Q19
        //System.out.println(optimizezr.registeredPushedUpSelections.map({ case (k, v) => (k.name, v) }).mkString(","))
        println(optimizerTree + "\n\n")

        executeQuery(optimizerTree, schema)

        // Check results
        if (Config.checkResults) {
          val getResultFileName = queryConf.resultsPath + currQuery + ".result_sf" + sf
          val resq = scala.io.Source.fromFile(getOutputName).mkString
          if (new java.io.File(getResultFileName).exists) {
            val resc = {
              val str = scala.io.Source.fromFile(getResultFileName).mkString
              str * Config.numRuns
            }
            if (resq != resc) {
              println("-----------------------------------------")
              println(args(1) + " QUERY" + q + " DID NOT RETURN CORRECT RESULT!!!")
              println("Correct result:")
              println(resc)
              println("Result obtained from execution:")
              println(resq)
              println("-----------------------------------------")
              System.exit(0)
            } else println("CHECK RESULT FOR " + args(1) + " QUERY " + q + ": [OK]")
          } else {
            println("Reference result file not found. Skipping checking of result")
            println("Execution results:")
            println(resq)
          }
        }
      }
    }
  }
}
