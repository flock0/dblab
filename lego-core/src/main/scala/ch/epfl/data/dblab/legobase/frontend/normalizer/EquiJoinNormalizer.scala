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
  //TODO Move stuff out here!
  override def normalize(stmt: SelectStatement): SelectStatement = {

    var equiPreds: Seq[Expression] = Seq.empty
    var otherPreds: Seq[Expression] = Seq.empty
    var usedPreds: Seq[Expression] = Seq.empty

    /**
     * Separates equality predicates between two fields
     * on the top level of the predicate operator tree
     * from all the other predicates.
     */
    def separateEquiJoinPredicates(wh: Expression]): (Seq[Equals], Seq[Expression]) = ex match {
      case And(left, right) => {
        val (e1, o1) = separateEquiJoinPredicates(left)
        val (e2, o2) = separateEquiJoinPredicates(right)
        (e1 ++ e2, o1 ++ o2)
      }
      case eq @ Equals(FieldIdent(_, _, _), FieldIdent(_, _, _)) => (Seq(eq), Seq.empty)
      case o @ _ => (Seq.empty, Seq(o))
    }

    def containsField(rel: Relation, field: Expression): Boolean = ??? //TODO Implement me!

    /**
     * Joins two relations by searching for predicate to match.
     * Throws an exception if no suitable predicate can be found.
     */
    def joinRelations(left: Relation, right: Relation, predicates: Seq[Equals]): Relation = {

      /* Could this be a predicate for the join? */
      val candidates = predicates.filter { eq =>
        (containsField(left, eq.left) && containsField(right, eq.right)) ||
          (containsField(left, eq.right) && containsField(right, eq.left))
      }

      if (candidates.size == 0)
        throw new Exception(s"LegoBase Frontend BUG: Couldn't find a candidate predicate for joining $left and $right!")
      if (candidates.size > 1)
        throw new Exception(s"LegoBase Frontend BUG: Found more than one candidate predicate for joining $left and $right! Please make the query unambiguous. ($candidates)")
      // Now we have a suitable join predicate
      // Find the correct order of the the fields in the predicate
      val pred = candidates(0)
      usedPreds :+= pred
      if (containsField(left, pred.left))
        Join(left, right, InnerJoin, pred)
      else
        Join(left, right, InnerJoin, Equals(pred.right, pred.left))
    }

    /** Removes predicates that have been used up by the joins */
    def purgePredicates =
      equiPreds.filter(!usedPreds.contains(_))

    /** Connects the remaining predicates using AND */
    def connectPredicates(eq: Seq[Equals], other: Seq[Expression]): Expression =
      (eq ++ other).reduce(And(_, _))

    val newJoinTree = stmt.joinTrees match {
      case None => throw new Exception("LegoBase Frontend BUG: Couldn't find any joinTree in the select statement!")
      case Some(jts) => {
        val (e, o) = separateEquiJoinPredicates(stmt.where) //TODO Handle Option correctly
        equiPreds = e
        otherPreds = o
        jts.reduceLeft((acc, right) => joinRelations(acc, right, equiPreds))
      }
    }
    val purgedPredicates = purgePredicates(equiPreds)
    val connectedPredicates = connectPredicates(purgedPredicates, otherPreds)
    //TODO Search through all relations and expressions to find all subqueries and recursively call this method

    SelectStatement(stmt.projections, stmt.relations, newJoinTree, connectedPredicates,
      stmt.groupBy, stmt.having, stmt.orderBy, stmt.limit, stmt.aliases)
  }
}