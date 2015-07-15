package ch.epfl.data
package dblab.legobase
package benchmarks

import schema.Schema
import frontend._

object TPCDSConfig extends BenchConfig {

  override val ddlPath: String = "tpcds/tpcds.ddl"
  override val constraintsPath: String = "tpcds/tpcds.ri"
  override val queryFolder: String = "tpcds/"
  override val checkResult: Boolean = false

  override val resultsPath: String = ""

  override def addStats(schema: Schema) = {}
}