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
object CTENormalizer extends Normalizer {
  override def normalize(stmt: SelectStatement): SelectStatement = {
    val appliedCTEs = stmt.withs.foldLeft(Seq[Subquery]())((agg, curr) => agg :+ inlineCTEsInCTE(agg, curr))
    inlineCTE(appliedCTEs, stmt)
  }

  /**
   * Inlines CTEs into another CTE of the query.
   * This method cannot be used for the main query itself.
   */
  private def inlineCTEsInCTE(existingCTEs: Seq[Subquery], currentCTE: Subquery): Subquery = {

    if (existingCTEs.map(_.alias) contains currentCTE.alias)
      throw new Exception(s"LegoBase Frontend BUG: WITH expression '${currentCTE.alias}' occured multiple times. Please use a different name.")

    Subquery(inlineCTE(existingCTEs, currentCTE.subquery), currentCTE.alias)
  }

  /** Inline CTEs into a select statement */
  private def inlineCTE(existingCTEs: Seq[Subquery], stmt: SelectStatement): SelectStatement = {
    val inlinedJoinTrees = stmt.joinTrees.get.map(replaceCTEsInJoinTree(existingCTEs, _))
    val extractedRelations = SQLParser.extractAllRelationsFromJoinTrees(stmt.joinTrees.get)

    SelectStatement(existingCTEs, stmt.projections, extractedRelations,
      Some(inlinedJoinTrees), stmt.where, stmt.groupBy, stmt.having,
      stmt.orderBy, stmt.limit, stmt.aliases)
  }

  /**
   * Recursively traverses a join tree to replace all
   * table qualifiers matching a CTE with the CTE itself.
   */
  private def replaceCTEsInJoinTree(existingCTEs: Seq[Subquery], jt: Relation): Relation = jt match {
    case Subquery(sq, al) => Subquery(inlineCTE(existingCTEs, sq), al)
    case Join(l, r, t, c) => Join(replaceCTEsInJoinTree(existingCTEs, l), replaceCTEsInJoinTree(existingCTEs, r), t, c)
    case tbl @ SQLTable(tblName, tblAlias) =>
      existingCTEs.find(cte => cte.alias == tblName) match {
        case Some(cte) => Subquery(cte.subquery, tblAlias.get)
        case None      => tbl
      }
  }
}