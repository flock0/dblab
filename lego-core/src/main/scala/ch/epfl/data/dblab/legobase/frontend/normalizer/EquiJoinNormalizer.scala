package ch.epfl.data
package dblab.legobase
package frontend
package normalizer

import frontend.SelectStatement
import schema.Schema
import scala.collection.mutable.ListBuffer
/**
 * Takes a select statement an pushes equijoin predicates in the WHERE
 * clause to the tables in the FROM clause. Also reorders the predicates
 * to match the order of the tables.
 */
class EquiJoinNormalizer(schema: Schema) extends Normalizer {
  //TODO Move stuff out here!
  override def normalize(stmt: SelectStatement): SelectStatement = {

    /**
     * Separates equality predicates between two fields
     * on the top level of the predicate operator tree
     * from all the other predicates.
     */
    def separateEquiJoinPredicates(wh: Expression): (Seq[Equals], Seq[Expression]) = wh match {
      case And(left, right) => {
        val (e1, o1) = separateEquiJoinPredicates(left)
        val (e2, o2) = separateEquiJoinPredicates(right)
        (e1 ++ e2, o1 ++ o2)
      }
      case eq @ Equals(FieldIdent(_, _, _), FieldIdent(_, _, _)) => (Seq(eq), Seq.empty)
      case o @ _ => (Seq.empty, Seq(o))
    }

    /** Checks if an expression (must be a FieldIdent) is a valid field in the relation */
    def containsField(rel: Relation, field: Expression): Boolean = field match {
      case FieldIdent(quali, fName, _) => rel match { /* Look only at FieldIdents */
        case SQLTable(tName, alias) => quali match {
          case None =>
            /* Just check if attribute exists in table */
            schema.findTable(tName).findAttribute(fName) match {
              case None    => false
              case Some(_) => true
            }
          case Some(q) => alias match {
            case None => schema.findTable(tName).findAttribute(fName) match {
              case None    => false
              case Some(_) => true
            }
            case Some(ali) => /* Check that the qualifier matches the alias */
              if (q != ali)
                false
              else
                schema.findTable(tName).findAttribute(fName) match {
                  case None    => false
                  case Some(_) => true
                }
          }
        }
        case Subquery(subquery, alias) => ??? //TODO Check the projections for a matching attribute
        case Join(left, right, tpe, _) => tpe match {
          /* For LeftSemi- and AntiJoin ignore the right relation, 
           * otherwise check both relations of the join */
          case LeftSemiJoin | AntiJoin => containsField(left, field)
          case _                       => containsField(left, field) || containsField(right, field)
        }
      }
      case _ => false
    }

    /**
     * Joins two relations by searching for predicate to match.
     * Throws an exception if no suitable predicate can be found.
     */
    def joinRelations(left: Relation, right: Relation, predicates: Seq[Equals], usedPreds: ListBuffer[Equals]): Relation = {

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
      usedPreds += pred
      if (containsField(left, pred.left))
        Join(left, right, InnerJoin, pred)
      else
        Join(left, right, InnerJoin, Equals(pred.right, pred.left))
    }

    /** Removes predicates that have been used up by the joins */
    def purgePredicates(equiPreds: Seq[Equals], usedPreds: Seq[Equals]) =
      equiPreds.filter(!usedPreds.contains(_))

    /** Connects the remaining predicates using AND */
    def connectPredicates(eq: Seq[Equals], other: Seq[Expression]): Option[Expression] =
      if (eq.size + other.size == 0)
        None
      else
        Some((eq ++ other).reduce(And(_, _)))

    stmt.joinTrees match {
      case None => throw new Exception("LegoBase Frontend BUG: Couldn't find any joinTree in the select statement!")
      case Some(jts) =>
        if (jts.size == 1) {
          stmt //Nothing to normalize
        } else {
          // Try to normalize to a single join tree
          var usedPreds = new ListBuffer[Equals]()
          val (equiPreds, otherPreds) = stmt.where match {
            case None =>
              throw new Exception("LegoBase limitation: Joins without a join condition are currently not supported.")
            case Some(wh) => separateEquiJoinPredicates(wh)
          }

          val newJoinTree = jts.reduceLeft((acc, right) => joinRelations(acc, right, equiPreds, usedPreds))
          val purgedPredicates = purgePredicates(equiPreds, usedPreds)
          val connectedPredicates = connectPredicates(purgedPredicates, otherPreds)
          //TODO Search through all relations and expressions to find all subqueries and recursively call this method

          SelectStatement(stmt.projections, stmt.relations, Some(Seq(newJoinTree)), connectedPredicates,
            stmt.groupBy, stmt.having, stmt.orderBy, stmt.limit, stmt.aliases)
        }

    }

  }
}