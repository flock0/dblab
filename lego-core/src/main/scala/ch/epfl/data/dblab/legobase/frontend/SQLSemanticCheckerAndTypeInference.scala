package ch.epfl.data
package dblab.legobase
package frontend

import sc.pardis.types._
import schema._
import scala.reflect.runtime.{ universe => ru }
import ru._

class SQLSemanticCheckerAndTypeInference(schema: Schema) {
  var aliasesList: Seq[(Expression, String, Int)] = _

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
      case (x, y) if x == y        => e.setTp(x)
      case (IntType, DoubleType)   => e.setTp(DoubleType)
      case (DoubleType, IntType)   => e.setTp(DoubleType)
      case (DoubleType, FloatType) => e.setTp(DoubleType)
    }
  }

  def checkAndInferExpr(expr: Expression): Unit = expr match {
    // Literals
    case lt @ (DateLiteral(_) | IntLiteral(_)) =>
      lt.setTp(typeTag[Int])
    case fl @ FloatLiteral(_) =>
      fl.setTp(typeTag[Float])
    case dl @ DoubleLiteral(_) =>
      dl.setTp(typeTag[Double])
    case sl @ StringLiteral(_) =>
      sl.setTp(typeTag[LBString])
    case cl @ CharLiteral(_) =>
      cl.setTp(typeTag[Char])
    case fi @ FieldIdent(_, name, _) =>
      schema.findAttribute(name) match {
        case Some(a) =>
          fi.setTp(typeToTypeTag(a.dataType))
        case None =>
          aliasesList.find(al => al._2 == name) match {
            case Some(al) => fi.setTp(al._1.tp)
            case None     => //throw new Exception("Attribute " + name + " referenced in SQL query does not exist in any relation.")
          }
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
    case div @ Divide(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      setResultType(div, left, right)
    case sum @ Sum(expr) =>
      checkAndInferExpr(expr)
      sum.setTp(typeTag(expr.tp))
    case avg @ Avg(expr) =>
      checkAndInferExpr(expr)
      avg.setTp(typeTag(expr.tp))
    case countAll @ CountAll() =>
      countAll.setTp(typeTag[Double])
    case countExpr @ CountExpr(expr) =>
      checkAndInferExpr(expr)
      countExpr.setTp(typeTag[Double])
    case min @ Min(expr) =>
      checkAndInferExpr(expr)
      min.setTp(expr.tp)
    // Logical Operators
    case and @ And(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      and.setTp(typeTag[Boolean])
    case and @ Or(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      and.setTp(typeTag[Boolean])
    case eq @ Equals(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      eq.setTp(typeTag[Boolean])
    case neq @ NotEquals(left, right) =>
      checkAndInferExpr(left)
      checkAndInferExpr(right)
      neq.setTp(typeTag[Boolean])
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
    // SQL Statements
    case yr @ Year(date) =>
      checkAndInferExpr(date)
      yr.setTp(typeTag[Int])
    case lk @ Like(field, values, _) =>
      checkAndInferExpr(field)
      checkAndInferExpr(values)
      lk.setTp(field.tp)
    case ex @ Exists(nestedQuery) =>
      checkAndInfer(nestedQuery)
      ex.setTp(nestedQuery.tp)
    case cs @ Case(cond, thenp, elsep) =>
      checkAndInferExpr(cond)
      checkAndInferExpr(thenp)
      checkAndInferExpr(elsep)
      setResultType(cs, thenp, elsep)
    case in @ In(fld, listExpr, _) =>
      checkAndInferExpr(fld)
      listExpr.foreach(e => checkAndInferExpr(e))
      in.setTp(typeTag[Boolean])
    case substr @ Substring(fld, idx1, idx2) =>
      checkAndInferExpr(fld)
      checkAndInferExpr(idx1)
      checkAndInferExpr(idx2)
      substr.setTp(fld.tp)
    case e: SelectStatement => // Nested subquery
      checkAndInfer(e)
  }

  def checkAndInferJoinTree(root: Relation): Unit = root match {
    case Join(leftParent, Subquery(subquery, _), _, _) => checkAndInfer(subquery)
    case Subquery(parent, _) => checkAndInfer(parent)
    case _ =>
  }

  def checkAndInfer(sqlTree: SelectStatement) {
    aliasesList = sqlTree.aliases
    sqlTree.where match {
      case Some(expr) => checkAndInferExpr(expr)
      case None       =>
    }
    sqlTree.projections match {
      case ExpressionProjections(proj) => proj.foreach(p => checkAndInferExpr(p._1))
      case AllColumns()                =>
    }
    sqlTree.groupBy match {
      case Some(GroupBy(listExpr)) => listExpr.map(_._1).foreach(expr => checkAndInferExpr(expr))
      case None                    =>
    }
    sqlTree.orderBy match {
      case Some(OrderBy(listExpr)) => listExpr.foreach(expr => checkAndInferExpr(expr._1))
      case None                    =>
    }
    sqlTree.joinTree match {
      case Some(tr) => checkAndInferJoinTree(tr)
      case None     =>
    }
    sqlTree.having match {
      case Some(clause) => checkAndInferExpr(clause.having)
      case None         =>
    }
  }
}