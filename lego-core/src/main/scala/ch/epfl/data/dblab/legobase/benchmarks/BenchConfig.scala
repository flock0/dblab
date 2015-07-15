package ch.epfl.data
package dblab.legobase
package benchmarks

import schema.Schema

trait BenchConfig {
  /** The path from the project root to the file containing the DDL statements*/
  def ddlPath: String
  /** The path from the project root to the file containing the constraints */
  def constraintsPath: String
  /** Whether to validate the query results */
  def checkResult: Boolean
  /** The path from the project root to the folder with results for validation */
  def resultsPath: String = ""
  /** Optimizes the operator tree (if any optimizations are applicable) */
  def optimize(schema: Schema, operatorTree: Operator): Operator 
  /** Parses and interprets the given DDL and constraints from disk */
  def interpret: Schema
  /** Optionally adds statistics to the schema */
  def addStats(schema: Schema)
}