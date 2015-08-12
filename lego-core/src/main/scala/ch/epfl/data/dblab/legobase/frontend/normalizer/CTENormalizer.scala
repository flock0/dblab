package ch.epfl.data
package dblab.legobase
package frontend
package normalizer

import frontend.SelectStatement

/**
 * Takes all common table expressions (CTEs) in the statement
 * and inlines them. They can occur in the table list of subsequent
 * CTEs or the table list of the main query itself.
 */
object CTENormalizer extends Normalizer{
  override def normalize(tree: SelectStatement): SelectStatement {
    val appliedCTEs = stmt.withs.foldLeft(Seq.empty)( (agg, curr) => agg ++ inlineCTEsInCTE(agg, curr)))
    ??? //TODO inline CTEs to main statement
  }

  /** Inlines CTEs into another CTE of the query. 
    * This method cannot be used for the main query itself.
    */
  private def inlineCTEsInCTE(existingCTEs: Seq[Subquery], currentCTE: Subquery): Subquery = {

    if (existingCTEs.map(q => q.alias) contains currentCTE.alias)
      throw new Exception(s"LegoBase Frontend BUG: WITH expression '${currentCTE.alias}' occured multiple times. Please use a different name.")

    Subquery(inlineCTE(existingCTEs, currentCTE.subquery), currentCTE.alias)
  }

  /** Inline CTEs into a select statement */
  private def inlineCTE(existingCTEs: Seq[Subquery], query: SelectStatement): SelectStatement = {
    val inlinedJoinTrees = query.joinTrees.map(jt => traverseJoinTree(jt))
    ??? //TODO Implement me
  }

  /** Recursively traverses a join tree to replace all 
    * table qualifiers matching a CTE with the CTE itself.
    */
  private def traverseJoinTree(existingCTEs: Seq[Subquery], jt: Relation): Relation = ??? //TODO Implement me
}