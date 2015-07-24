package ch.epfl.data
package dblab.legobase
package frontend
package normalizer

import frontend.Node

/* The general interface each SQL normalizer of legobase must abide to.
 * Observe that since the main normalize function takes as input and returns 
 * an SQLAST, this allows normalizers to be chained together. */
trait Normalizer {
  def normalize(tree: Node): Node
}