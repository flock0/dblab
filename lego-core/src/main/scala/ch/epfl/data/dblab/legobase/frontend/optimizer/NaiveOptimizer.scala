package ch.epfl.data
package dblab.legobase
package frontend
package optimizer

import schema._
import OperatorAST._

/* This optimizer is called naive, since it only pushes-up selection predicates to
 * scan operators, and performs no other technique from traditional query optimization
 * (e.g. join reordering). 
 *
 * The tree received by this optimizer is the one built by the SQL parser, thus 
 * _by construction_ has _a single_ select op that performs the where clause, and
 * which is executed after all the scans and joins and aggregations have been 
 * executed. This optimizer pushes up parts of this where clause to the corresponding 
 * scan operators.
 */
class NaiveOptimizer(schema: Schema) extends Optimizer {

  var linkingOperatorIsAnd = true
  var operatorList: List[OperatorNode] = _
  val registeredPushedUpSelections = new scala.collection.mutable.HashMap[OperatorNode, Expression]();

  def registerPushUp(cond: Expression, fi: FieldIdent) {
    val t = schema.tables.find(t => t.findAttribute(fi.name).isDefined) match {
      case Some(t) => t
      case None    => throw new Exception("BUG: Attribute " + fi.name + " referenced but no table found with it!")
    }
    val scanOperators = operatorList.filter(_.isInstanceOf[ScanOpNode[_]]).map(_.asInstanceOf[ScanOpNode[_]])
    val scanOpName = t.name + fi.qualifier.getOrElse("")
    val scanOp = scanOperators.find(so => so.scanOpName == scanOpName) match {
      case Some(op) => op
      case None     => throw new Exception("BUG: Scan op " + scanOpName + " referenced but no such operator exists!")
    }
    registeredPushedUpSelections.get(scanOp) match {
      case None => registeredPushedUpSelections += scanOp -> dealiasFieldIdent(cond)
      case Some(c) => registeredPushedUpSelections += scanOp -> {
        if (linkingOperatorIsAnd) And(c, dealiasFieldIdent(cond))
        else Or(c, dealiasFieldIdent(cond))
      }
    }
  }

  def isPrimitiveExpression(expr: Expression) = expr match {
    case Equals(_, _) | LessThan(_, _) | LessOrEqual(_, _) | GreaterThan(_, _) | GreaterOrEqual(_, _) | Like(_, _, _) => true
    case _ => false
  }

  def dealiasFieldIdent(expr: Expression): Expression = {
    val newExpr = expr match {
      case FieldIdent(qualifier, name, symbol) => FieldIdent(None, name, symbol)
      case And(lhs, rhs)                       => And(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case Or(lhs, rhs)                        => Or(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case Equals(lhs, rhs)                    => Equals(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case LessThan(lhs, rhs)                  => LessThan(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case LessOrEqual(lhs, rhs)               => LessOrEqual(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case GreaterThan(lhs, rhs)               => GreaterThan(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case GreaterOrEqual(lhs, rhs)            => GreaterOrEqual(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs))
      case Like(lhs, rhs, negate)              => Like(dealiasFieldIdent(lhs), dealiasFieldIdent(rhs), negate)
      case c: LiteralExpression                => expr
    }
    newExpr.setTp(expr.tp)
    newExpr
  }

  def processPrimitiveExpression(expr: Expression) = expr match {
    case Equals((fi: FieldIdent), _)                                => fi
    case LessThan((fi: FieldIdent), _)                              => fi
    case LessOrEqual((fi: FieldIdent), (lit: LiteralExpression))    => fi
    case GreaterThan((fi: FieldIdent), _)                           => fi
    case GreaterOrEqual((fi: FieldIdent), (lit: LiteralExpression)) => fi
    case Like((fi: FieldIdent), (lit: LiteralExpression), _)        => fi
  }

  def analysePushingUpCondition(parent: OperatorNode, cond: Expression): Option[Expression] = cond match {
    case And(lhs, rhs) =>
      (isPrimitiveExpression(lhs), isPrimitiveExpression(rhs)) match {
        case (true, true) =>
          registerPushUp(lhs, processPrimitiveExpression(lhs));
          registerPushUp(rhs, processPrimitiveExpression(rhs));
          None
        case (false, true) =>
          analysePushingUpCondition(parent, lhs) match {
            case None =>
              registerPushUp(rhs, processPrimitiveExpression(rhs));
              None
            case Some(expr) =>
              Some(And(expr, rhs))
          }
        case (true, false) =>
          analysePushingUpCondition(parent, rhs) match {
            case None =>
              registerPushUp(lhs, processPrimitiveExpression(lhs));
              None
            case Some(expr) =>
              Some(And(lhs, expr))
          }
        case (false, false) =>
          (analysePushingUpCondition(parent, lhs), analysePushingUpCondition(parent, rhs)) match {
            case (None, None) => None
          }
      }
    case Or(lhs, rhs) =>
      linkingOperatorIsAnd = false
      val res = (isPrimitiveExpression(lhs), isPrimitiveExpression(rhs)) match {
        case (true, true) =>
          registerPushUp(lhs, processPrimitiveExpression(lhs));
          registerPushUp(rhs, processPrimitiveExpression(rhs));
          None
        case (false, false) =>
          (analysePushingUpCondition(parent, lhs), analysePushingUpCondition(parent, rhs)) match {
            case (None, None) => Some(Or(lhs, rhs))
          }
      }
      linkingOperatorIsAnd = true
      res
    case c if isPrimitiveExpression(c) => Some(c)
  }

  def pushUpCondition(tree: OperatorNode): OperatorNode = tree match {
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      JoinOpNode(pushUpCondition(leftParent), pushUpCondition(rightParent), joinCond, joinType, leftAlias, rightAlias)
    case ScanOpNode(table, _, _, _) => registeredPushedUpSelections.get(tree) match {
      case Some(expr) =>
        SelectOpNode(tree, expr, false)
      case None => tree
    }
    case SubqueryNode(parent) => SubqueryNode(optimizeInContext(parent))
  }

  def optimizeInContext(node: OperatorNode): OperatorNode = {
    // Save old map, then start processing the given node with an empty map and finally
    // restore original map with proceeding with rest of original query. Return the result
    // of processing the given node. This method is useful for subqueries
    val oldMap = registeredPushedUpSelections
    registeredPushedUpSelections.clear()
    val res = optimizeNode(node)
    registeredPushedUpSelections.clear()
    registeredPushedUpSelections ++= oldMap
    res
  }

  def optimizeNode(tree: OperatorNode): OperatorNode = tree match {
    case ScanOpNode(_, _, _, _) => tree
    case SelectOpNode(parent, cond, isHaving) if isHaving == false =>
      //System.out.println("SelectOp found with condition " + cond);
      analysePushingUpCondition(parent, cond) match {
        case Some(expr) if isPrimitiveExpression(expr) =>
          registerPushUp(cond, processPrimitiveExpression(expr))
          pushUpCondition(parent)
        case Some(expr) => SelectOpNode(pushUpCondition(parent), expr, false)
        case None       => pushUpCondition(parent)
      }
    case SelectOpNode(parent, cond, isHaving) if isHaving == true =>
      SelectOpNode(optimizeNode(parent), cond, isHaving)
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      JoinOpNode(optimizeInContext(leftParent), optimizeInContext(rightParent), joinCond, joinType, leftAlias, rightAlias)
    case SubquerySingleResultNode(parent)      => SubquerySingleResultNode(optimizeNode(parent))
    case MapOpNode(parent, mapIndices)         => MapOpNode(optimizeNode(parent), mapIndices)
    case AggOpNode(parent, aggs, gb, aggAlias) => AggOpNode(optimizeNode(parent), aggs, gb, aggAlias)
    case OrderByNode(parent, ob)               => OrderByNode(optimizeNode(parent), ob)
    case PrintOpNode(parent, projNames, limit) => PrintOpNode(optimizeNode(parent), projNames, limit)
    case SubqueryNode(parent)                  => SubqueryNode(optimizeNode(parent))
  }

  def optimize(tree: OperatorNode): OperatorNode = {
    operatorList = tree.toList
    optimizeNode(tree)
  }
}