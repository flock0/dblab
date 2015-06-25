package ch.epfl.data
package dblab.legobase
package frontend
package optimizer

import OperatorAST._

/* The general interface each query optimizer of legobase must abide to.
 * Observe that since the main optimize function takes as input and returns 
 * an operator tree, this allows optimizers to be chained together. */
trait Optimizer {
  def optimize(tree: OperatorNode): OperatorNode
}