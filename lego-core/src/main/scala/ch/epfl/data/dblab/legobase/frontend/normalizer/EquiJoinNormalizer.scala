package ch.epfl.data
package dblab.legobase
package frontend
package normalizer

import frontend.SelectStatement

/**
 * Takes a select statement an pushes equijoin predicates in the WHERE
 * clause to the tables in the FROM clause. Also reorders the predicates
 * to match the order of the tables.
 */
object EquiJoinNormalizer extends Normalizer {
  override def normalize(tree: SelectStatement): SelectStatement = tree //TODO
}