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
    case c if c == CharType                 => typeTag[Char]
    case c: VarCharType                     => typeTag[VarCharType]
  }

  def setResultType(e: Expression, left: Expression, right: Expression) {
    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    (left.tp, right.tp) match {
      case (IntType, DoubleType)    => e.setTp(DoubleType)
      case (DoubleType, DoubleType) => e.setTp(DoubleType)
    }
  }

  def checkAndInferExpr(expr: Expression): Unit = expr match {
    // Literals
    case lt @ (DateLiteral(_) | IntLiteral(_)) =>
      lt.setTp(typeTag[Int])
    case fl @ FloatLiteral(_) =>
      fl.setTp(typeTag[Float])
    case sl @ StringLiteral(_) =>
      sl.setTp(typeTag[String])
    case fi @ FieldIdent(_, name, _) =>
      schema.findAttribute(name) match {
        case Some(a) =>
          fi.setTp(typeToTypeTag(a.dataType))
        case None => throw new Exception("Attribute " + name + " referenced in SQL query does not exist in any relation.")
      }
    // Arithmetic Operators
    case add @ Add(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(add, left, right)
    case sub @ Subtract(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(sub, left, right)
    case mut @ Multiply(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(mut, left, right)
    case sum @ Sum(expr) =>
      checkAndInferExpr(expr)
      sum.setTp(typeTag(expr.tp))
    case avg @ Avg(expr) =>
      checkAndInferExpr(expr)
      avg.setTp(typeTag(expr.tp))
    case avg @ CountAll() =>
      avg.setTp(typeTag[Double])
    // Logical Operators
    case and @ And(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      and.setTp(typeTag[Boolean])
    case eq @ Equals(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      eq.setTp(typeTag[Boolean])
    case lt @ LessThan(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      lt.setTp(typeTag[Boolean])
    case loe @ LessOrEqual(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      loe.setTp(typeTag[Boolean])
    case gt @ GreaterThan(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      gt.setTp(typeTag[Boolean])
    case goe @ GreaterOrEqual(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      goe.setTp(typeTag[Boolean])
  }

  def checkAndInfer(sqlTree: SelectStatement) {
    sqlTree.where match {
      case Some(expr) => checkAndInferExpr(expr)
      case None       =>
    }
    sqlTree.projections match {
      case ExpressionProjections(proj) => proj.foreach(p => checkAndInferExpr(p._1))
    }
    sqlTree.groupBy match {
      case Some(GroupBy(listExpr, having)) => listExpr.foreach(expr => checkAndInferExpr(expr))
      case None                            =>
    }
  }
}