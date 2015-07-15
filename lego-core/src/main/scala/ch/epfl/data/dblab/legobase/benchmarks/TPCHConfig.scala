package ch.epfl.data
package dblab.legobase
package benchmarks

import schema.Schema
import frontend._

object TPCHConfig extends BenchConfig {

  override val ddlPath: String = "tpch/dss.ddl"
  override val constraintsPath: String = "tpch/dss.ri"
  override val checkResult: Boolean = true

  override val resultsPath: String = "results/"

  override def interpret: Schema = {
    val ddlDefStr = scala.io.Source.fromFile("tpch/dss.ddl").mkString
    val schemaDDL = DDLParser.parse(ddlDefStr)
    val constraintsDefStr = scala.io.Source.fromFile("tpch/dss.ri").mkString
    val constraintsDDL = DDLParser.parse(constraintsDefStr)
    val schemaWithConstraints = schemaDDL ++ constraintsDDL
    DDLInterpreter.interpret(schemaWithConstraints)
  }

  override def addStats(schema: Schema) = {
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
  }
}