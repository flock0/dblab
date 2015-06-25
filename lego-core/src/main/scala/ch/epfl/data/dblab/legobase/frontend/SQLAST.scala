package ch.epfl.data
package dblab.legobase
package frontend

import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * AST for SQL select statement.
 * Based on: https://github.com/stephentu/scala-sql-parser
 */

trait Node
case class SelectStatement(projections: Projections,
                           relations: Seq[Relation],
                           joinTree: Option[Relation],
                           where: Option[Expression],
                           groupBy: Option[GroupBy],
                           having: Option[Having],
                           orderBy: Option[OrderBy],
                           limit: Option[Limit],
                           aliases: Seq[(Expression, String, Int)]) extends Node with Expression {
  override def toString() = {
    "      SQL TREE\n====================" +
      "\n\tPROJECTIONS      :" + projections.toString +
      "\n\tRELATIONS        :" + relations.mkString(",") +
      "\n\tJOINTREE         :" + joinTree.getOrElse("").toString +
      "\n\tWHERE CLAUSES    :" + where.getOrElse("").toString +
      "\n\tGROUP BY CLAUSES :" + groupBy.getOrElse("").toString +
      "\n\tHAVING           :" + having.getOrElse("").toString +
      "\n\tORDER BY CLAUSES :" + orderBy.getOrElse("").toString +
      "\n\tLIMIT CLAUSE     :" + limit.toString +
      "\n\tALIASES          :" + aliases.mkString(",") +
      "\n===================="
  }
}

trait Projections extends Node
case class ExpressionProjections(lst: Seq[(Expression, Option[String])]) extends Projections
case class AllColumns() extends Projections

trait Expression extends Node {
  var tp: TypeTag[_] = null
  def getTp = tp match {
    case t if t == null => throw new Exception("SQL Type Inferrence BUG: type of Expression cannot be null.")
    case _              => tp
  }
  def setTp[A](tt: TypeTag[A]) { this.tp = tt }
}

trait BinaryOperator extends Expression {
  val left: Expression
  val right: Expression
}
case class Or(left: Expression, right: Expression) extends BinaryOperator
case class And(left: Expression, right: Expression) extends BinaryOperator

trait EqualityOperator extends BinaryOperator
case class Equals(left: Expression, right: Expression) extends EqualityOperator
case class NotEquals(left: Expression, right: Expression) extends EqualityOperator

trait InEqualityOperator extends BinaryOperator
case class LessOrEqual(left: Expression, right: Expression) extends InEqualityOperator
case class LessThan(left: Expression, right: Expression) extends InEqualityOperator
case class GreaterOrEqual(left: Expression, right: Expression) extends InEqualityOperator
case class GreaterThan(left: Expression, right: Expression) extends InEqualityOperator
case class In(elem: Expression, set: Seq[Expression], negate: Boolean) extends Expression

case class Like(left: Expression, right: Expression, negate: Boolean) extends BinaryOperator
case class Add(left: Expression, right: Expression) extends BinaryOperator
case class Subtract(left: Expression, right: Expression) extends BinaryOperator
case class Multiply(left: Expression, right: Expression) extends BinaryOperator
case class Divide(left: Expression, right: Expression) extends BinaryOperator

trait UnaryOperation extends Expression {
  val expr: Expression
}
case class Not(expr: Expression) extends UnaryOperation
case class UnaryPlus(expr: Expression) extends UnaryOperation
case class UnaryMinus(expr: Expression) extends UnaryOperation
case class Exists(select: SelectStatement) extends Expression

case class Case(cond: Expression, thenp: Expression, elsep: Expression) extends Expression

case class FieldIdent(qualifier: Option[String], name: String, symbol: Symbol = null) extends Expression {
  override def toString = qualifier match {
    case Some(q) => q + "." + name
    case None    => name
  }
}

trait Aggregation extends Expression
case class CountAll() extends Aggregation
case class CountExpr(expr: Expression) extends Aggregation
case class Sum(expr: Expression) extends Aggregation
case class Avg(expr: Expression) extends Aggregation
case class Min(expr: Expression) extends Aggregation
case class Max(expr: Expression) extends Aggregation
case class Year(expr: Expression) extends Expression
case class Substring(expr: Expression, idx1: Expression, idx2: Expression) extends Expression

trait LiteralExpression extends Expression
case class IntLiteral(v: Int) extends LiteralExpression {
  override def toString = v.toString
}
case class DoubleLiteral(v: Double) extends LiteralExpression {
  override def toString = v.toString
}
case class FloatLiteral(v: Float) extends LiteralExpression {
  override def toString = v.toString
}
case class StringLiteral(v: LBString) extends LiteralExpression {
  override def toString = "'" + v.toString + "'"
}
case class CharLiteral(v: Char) extends LiteralExpression {
  override def toString = v.toString
}
case class NullLiteral() extends LiteralExpression
case class DateLiteral(v: Int) extends LiteralExpression {
  override def toString = "DATE '" + v.toString + "'"
}

trait Relation extends Node
case class SQLTable(name: String, alias: Option[String]) extends Relation
case class Subquery(subquery: SelectStatement, alias: String) extends Relation

sealed abstract trait JoinType
case object InnerJoin extends JoinType
case object LeftSemiJoin extends JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object FullOuterJoin extends JoinType
case object AntiJoin extends JoinType

case class Join(left: Relation, right: Relation, tpe: JoinType, clause: Expression) extends Relation

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class GroupBy(keys: Seq[(Expression, Option[String])]) extends Node
case class Having(having: Expression) extends Node
case class OrderBy(keys: Seq[(Expression, OrderType)]) extends Node
case class Limit(rows: Long) extends Node
