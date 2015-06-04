package ch.epfl.data
package dblab.legobase
package frontend

import schema._
import scala.reflect.runtime.{ universe => ru }
import ru._

class SQLSemanticCheckerAndTypeInference(schema: Schema) {
  def checkAndInferExpr(expr: Expression): Unit = expr match {
    /*case And(left, right) =>
      parseExpression[Boolean](left) && parseExpression[Boolean](right)
    case LessThan(left, right) =>
      parseExpression[Double](left) < parseExpression[Long](right)
    case LessOrEqual(left, right) =>
      parseExpression[Double](left) <= parseExpression[Long](right)
    case GreaterThan(left, right) =>
      parseExpression[Double](left) > parseExpression[Long](right)*/
    case goe @ GreaterOrEqual(left, right) =>
      goe.setTp(typeTag[Boolean])
      checkAndInferExpr(left)
      checkAndInferExpr(right)
    case dl @ DateLiteral(v) =>
      dl.setTp(typeTag[Int])
    // System.out.println("here" + left.tp + "/")
    // val e = parseExpression(left) //>= parseExpression[Long](right)
    //System.out.println(e.getClass)
    /*case Add(left, right) =>
      parseExpression[Long](left) + parseExpression[Long](right)
    case Subtract(left, right) =>
      parseExpression[Long](left) - parseExpression[Long](right)
    case IntLiteral(v)   => v
    case FloatLiteral(v) => v.toLong
    //case bo: BinaryOperator => parseBinaryOperator(bo)*/
    case fi @ FieldIdent(_, name, _) =>
      schema.findAttribute(name) match {
        case Some(a) => fi.setTp(typeTag[Int]) //fi.setTp(a.dataType)
        case None    => throw new Exception("Attribute " + name + " referenced in SQL query does not exist in any relation.")
      }
    //case le: LiteralExpression => parseLiteralExpression(le)
  }

  def checkAndInfer(sqlTree: SelectStatement) {
    sqlTree.where match {
      case Some(expr) => checkAndInferExpr(expr)
      case None       =>
    }
  }
}