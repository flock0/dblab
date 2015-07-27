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
  override def normalize(tree: SelectStatement): SelectStatement = {
    val moved = moveJoinPredicates(tree)
    reorderJoinPredicates(tree)
  }

  private def moveJoinPredicates(stmt: SelectStatement): SelectStatement = {
    /** Separates equality predicates between two fields
      * on the top level of the predicate operator tree
      * from all the other predicates.
      */
    def separateEquiJoinPredicates(wh: Expression): (Seq[Equals], Seq[Expression]) = wh match {
        case And(left, right) => extractEquiJoinPredicates(left) ++ extractEquiJoinPredicates(right)
        case eq @ Equals(_,_) => Seq(eq)
        case _ => Seq.empty
      }

    /** Joins two relations by searching for predicate to match.
      * Throws an exception if no suitable predicate can be found.
      */
    def joinRelations(left: Relation, right: Relation, predicates: Seq[Equals]) = ??? //TODO Implement me!
    /** Removes predicates that have been used up by the joins */
    def purgePredicates(preds: Seq[Equals]) = ??? //TODO Implement me!
    /** Connects the remaining predicates using AND */
    def connectPredicates(eq: Seq[Equals], other: Seq[Expression]) = ??? //TODO Implement me!
    
    tree match {
        case SelectStatement(pr, rel, jTrees, wh, grp, hav, ord, lim, ali) => {
          val newJoinTree = stmt.joinTrees match {
            case None => throw new Exception("EquiJoinNormalizer BUG: Couldn't find any joinTree in the select statement!")
            case Some(jts) => {
              val (equiPreds, otherPreds) = separateEquiJoinPredicates(wh)
              jts.reduceLeft((acc, right) => joinRelations(acc, right, predicates)))
            }
          }
          val purgedPredicates = purgePredicates(predicates)
          val connectedPredicates = connectPredicates(purgedPredicates)
          //TODO Search through all relations and expressions to find all subqueries and recursively call this method

        }
    }
  }
}