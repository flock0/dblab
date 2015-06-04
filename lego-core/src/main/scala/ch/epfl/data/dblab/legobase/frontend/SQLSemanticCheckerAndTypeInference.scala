package ch.epfl.data
package dblab.legobase
package frontend

import sc.pardis.types._
import schema._
import scala.reflect.runtime.{ universe => ru }
import ru._

class SQLSemanticCheckerAndTypeInference(schema: Schema) {
  // TODO: Maybe this should be removed if there is a better solution for it
  def typeToTypeTag(tp: Tpe) = tp match {
    case c if c == IntType || c == DateType => typeTag[Int]
    case c if c == DoubleType               => typeTag[Double]
  }

  def checkAndInferExpr(expr: Expression): Unit = expr match {
    case and @ And(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      and.setTp(typeTag[Boolean])
    case lt @ LessThan(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      lt.setTp(typeTag[Boolean])
    case loe @ LessOrEqual(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      loe.setTp(typeTag[Boolean])
    /*case GreaterThan(left, right) =>
      parseExpression[Double](left) > parseExpression[Long](right)*/
    case goe @ GreaterOrEqual(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      goe.setTp(typeTag[Boolean])
    case lt @ (DateLiteral(_) | IntLiteral(_)) =>
      lt.setTp(typeTag[Int])
    case fl @ FloatLiteral(_) =>
      fl.setTp(typeTag[Float])
    case add @ Add(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      add.setTp(typeTag(left.tp))
    case sub @ Subtract(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      sub.setTp(typeTag(left.tp))

    // System.out.println("here" + left.tp + "/")
    // val e = parseExpression(left) //>= parseExpression[Long](right)
    //System.out.println(e.getClass)

    /*case IntLiteral(v)   => v
    case FloatLiteral(v) => v.toLong
    //case bo: BinaryOperator => parseBinaryOperator(bo)*/
    case fi @ FieldIdent(_, name, _) =>
      schema.findAttribute(name) match {
        case Some(a) => {
          fi.setTp(typeToTypeTag(a.dataType))
        }
        case None => throw new Exception("Attribute " + name + " referenced in SQL query does not exist in any relation.")
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