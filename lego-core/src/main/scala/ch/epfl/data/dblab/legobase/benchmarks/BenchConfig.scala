package ch.epfl.data
package dblab.legobase
package benchmarks

import schema.Schema

trait BenchConfig {
  /** The path from the project root to the file containing the DDL statements*/
  def ddlPath: String
  /** The path from the project root to the file containing the constraints */
  def constraintsPath: String
  /** The path from the project root to the folder containing the SQL queries */
  def queryFolder: String
  /** Whether to validate the query results */
  def checkResult: Boolean
  /** The path from the project root to the folder with results for validation */
  def resultsPath: String = ""
  /** Optionally adds statistics to the schema */
  def addStats(schema: Schema)
}