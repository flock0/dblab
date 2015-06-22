package ch.epfl.data
package dblab.legobase
package frontend

import scala.reflect.runtime.{ universe => ru }
import ru._

/**
 * AST for SQL select statement.
 * Based on: https://github.com/stephentu/scala-sql-parser
 */

abstract trait Node
case class SelectStatement(withs: List[WithExpression],
                           projections: Projections,
                           relations: Seq[Relation],
                           joinTree: Option[Relation],
                           where: Option[Expression],
                           groupBy: Option[GroupBy],
                           orderBy: Option[OrderBy],
                           limit: Option[Limit],
                           aliases: Seq[(Expression, String, Int)]) extends Node with Expression

case class WithExpression(name: String, columns: Option[List[String]], stmt: SelectStatement) extends Node with Expression {
  def gatherFields = Seq.empty // TODO: not sure about that
}
abstract trait Projections extends Node
case class ExpressionProjections(lst: Seq[(Expression, Option[String])]) extends Projections
case class AllColumns() extends Projections

trait Expression extends Node {
  var tp: TypeTag[_] = null
  def getTp = tp match {
    case t if t == null => throw new Exception("SQL Type Inferrence BUG: type of Expression cannot be null.")
    case _              => tp
  }
  def setTp[A](tt: TypeTag[A]) { this.tp = tt }

  def isLiteral: Boolean = false

  // is the r-value of this expression a literal?
  def isRValueLiteral: Boolean = isLiteral
}

abstract trait BinaryOperator extends Expression {
  val left: Expression
  val right: Expression

  override def isLiteral = left.isLiteral && right.isLiteral
}
case class Or(left: Expression, right: Expression) extends BinaryOperator
case class And(left: Expression, right: Expression) extends BinaryOperator

abstract trait EqualityOperator extends BinaryOperator
case class Equals(left: Expression, right: Expression) extends EqualityOperator
case class NotEquals(left: Expression, right: Expression) extends EqualityOperator

abstract trait InEqualityOperator extends BinaryOperator
case class LessOrEqual(left: Expression, right: Expression) extends InEqualityOperator
case class LessThan(left: Expression, right: Expression) extends InEqualityOperator
case class GreaterOrEqual(left: Expression, right: Expression) extends InEqualityOperator
case class GreaterThan(left: Expression, right: Expression) extends InEqualityOperator
case class In(elem: Expression, set: Seq[Expression], negate: Boolean) extends Expression {
  override def isLiteral =
    elem.isLiteral && set.filter(e => !e.isLiteral).isEmpty
}
case class Like(left: Expression, right: Expression, negate: Boolean) extends BinaryOperator
case class Add(left: Expression, right: Expression) extends BinaryOperator
case class Subtract(left: Expression, right: Expression) extends BinaryOperator
case class Multiply(left: Expression, right: Expression) extends BinaryOperator
case class Divide(left: Expression, right: Expression) extends BinaryOperator

abstract trait UnaryOperation extends Expression {
  val expr: Expression
  override def isLiteral = expr.isLiteral
}
case class Not(expr: Expression) extends UnaryOperation
case class UnaryPlus(expr: Expression) extends UnaryOperation
case class UnaryMinus(expr: Expression) extends UnaryOperation
case class Exists(select: SelectStatement) extends Expression

case class Case(cond: Expression, thenp: Expression, elsep: Expression) extends Expression

case class FieldIdent(qualifier: Option[String], name: String, symbol: Symbol = null) extends Expression

abstract trait Aggregation extends Expression
case class CountAll() extends Aggregation {
  def gatherFields = Seq.empty
}
case class CountExpr(expr: Expression, distinct: Boolean) extends Aggregation {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
}
case class Sum(expr: Expression, distinct: Boolean) extends Aggregation {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
}
case class Avg(expr: Expression, distinct: Boolean) extends Aggregation {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
}
case class Min(expr: Expression, distinct: Boolean) extends Aggregation {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
}
case class Max(expr: Expression, distinct: Boolean) extends Aggregation {
  def gatherFields = expr.gatherFields.map(_.copy(_2 = true))
}

abstract trait Function extends Expression
case class Extract(what: ExtractType, from: Expression) extends Function {
  def gatherFields = from.gatherFields
}
case class Substring(expr: Expression, start: Int, end: Int) extends Function {
  def gatherFields = expr.gatherFields
}

sealed abstract trait CaseExpression extends Expression
case class SimpleCaseExpression(input: Expression, cases: Seq[CaseExpressionCase], default: Option[Expression]) extends CaseExpression {
  def gatherFields = input.gatherFields ++ cases.flatMap(_.gatherFields) ++ (default match {
    case None    => Seq()
    case Some(e) => e.gatherFields
  })
}
case class ComplexCaseExpression(cases: Seq[CaseExpressionCase], default: Option[Expression]) extends CaseExpression {
  def gatherFields = cases.flatMap(_.gatherFields) ++ (default match {
    case None    => Seq[(FieldIdent, Boolean)]()
    case Some(e) => e.gatherFields
  })
}

case class CaseExpressionCase(whenExpr: Expression, thenExpr: Expression) {
  def gatherFields = whenExpr.gatherFields ++ thenExpr.gatherFields
}

abstract trait LiteralExpression extends Expression {
  override def isLiteral = true
}
case class IntLiteral(v: Int) extends LiteralExpression
case class DoubleLiteral(v: Double) extends LiteralExpression
case class FloatLiteral(v: Float) extends LiteralExpression
case class StringLiteral(v: LBString) extends LiteralExpression
case class CharLiteral(v: Char) extends LiteralExpression
case class NullLiteral() extends LiteralExpression
case class DateLiteral(d: Int) extends LiteralExpression

trait Relation extends Node
case class SQLTable(name: String, alias: Option[String]) extends Relation
case class Subquery(subquery: SelectStatement, alias: String) extends Relation

sealed abstract trait JoinType
case object InnerJoin extends JoinType
case object LeftSemiJoin extends JoinType
case object LeftOuterJoin extends JoinType
case object RightOuterJoin extends JoinType
case object FullOuterJoin extends JoinType

case class Join(left: Relation, right: Relation, tpe: JoinType, clause: Expression) extends Relation

sealed abstract trait OrderType
case object ASC extends OrderType
case object DESC extends OrderType

case class GroupBy(keys: Seq[(Expression, Option[String])], having: Option[Expression]) extends Node
case class OrderBy(keys: Seq[(Expression, OrderType)]) extends Node
case class Limit(rows: Long) extends Node

sealed abstract trait ExtractType
case object YEAR extends ExtractType
case object MONTH extends ExtractType
case object DAY extends ExtractType
