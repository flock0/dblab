package ch.epfl.data
package dblab.legobase
package tpch

import queryengine._
/**
 *   In order to change from pull engine to push engine the next line should be commented and the line after
 *   should be uncommented.
 */
// import queryengine.volcano._
import queryengine.push._
import sc.pardis.annotations.{ deep, metadeep, dontLift }
import GenericEngine._
import CatalogTPCHLoader._
import storagemanager._

@deep
trait CatalogQueries

/**
 * A module containing 22 TPCH queries
 */
object CatalogQueries {

  def Q1(numRuns: Int) = ???

  def Q2(numRuns: Int) = ???

  def Q3(numRuns: Int) = ???

  def Q4(numRuns: Int) = ???

  def Q5(numRuns: Int) = ???

  def Q6(numRuns: Int) = ???

  def Q7(numRuns: Int) = ???

  def Q8(numRuns: Int) = ???

  def Q9(numRuns: Int) = ???

  def Q10(numRuns: Int) = ???

  def Q11(numRuns: Int) = ???

  def Q12(numRuns: Int) = ???

  def Q13(numRuns: Int) = ???

  def Q14(numRuns: Int) = ???

  def Q15(numRuns: Int) = ???
  
  def Q16(numRuns: Int) = ???

  def Q17(numRuns: Int) = ???

  // Danger, Will Robinson!: Query takes a long time to complete in Scala (but we 
  // knew that already)
  def Q18(numRuns: Int) = ???

  def Q19(numRuns: Int) = ???

  def Q20(numRuns: Int) = ???

  def Q21(numRuns: Int) = ???

  def Q22(numRuns: Int) = ???
}
