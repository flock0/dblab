package ch.epfl.data
package dblab.legobase
package frontend

import utils._
import Numeric.Implicits._
import Ordering.Implicits._
import schema._
import storagemanager._
import ch.epfl.data.dblab.legobase.queryengine.push._
import scala.collection.mutable.HashMap
import sc.pardis.shallow.CaseClassRecord
import scala.language.implicitConversions

// TODO: The rest we should not need when we generalize
import tpch._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

class SQLTreeToQueryPlanConverter(schema: Schema) {
  val tableMap = new HashMap[String, Array[_]]()
  var currBindVar: CaseClassRecord = _

  def loadRelations(sqlTree: SelectStatement) {
    val sqlRelations = sqlTree.relations;
    val schemaRelations = sqlRelations.map(r => schema.findTable(r match {
      case t: Table    => t.name
      case s: Subquery => s.alias
    }))
    schemaRelations.foreach(tn => tableMap += tn.name -> {
      // TODO: Generalize
      Loader.loadTable[LINEITEMRecord](tn)(classTag[LINEITEMRecord])
    })
  }

  class NumType

  def parseNumericExpression[A: Numeric: TypeTag](e: Expression): A = (e match {
    case FieldIdent(_, name, _) => currBindVar.getField(name).get
    case DateLiteral(v)         => v
    case FloatLiteral(v)        => v
    case IntLiteral(v)          => v
    case _                      => parseExpression(e)
  }).asInstanceOf[A]

  /*def typeTagToNumeric[A](tp: TypeTag[A]): Numeric[A] = (tp match {
    case x if x == typeTag[Int] => implicitly[Numeric[Int]]
  }).asInstanceOf[Numeric[A]]*/

  //sdef typeNum[T](tp: TypeTag[T]) = tp.asInstanceOf[TypeTag[NumType]]

  def getNumVal[T](tp: TypeTag[T]): Numeric[NumType] = (tp match {
    case x if x == typeTag[Int]    => implicitly[Numeric[Int]]
    case x if x == typeTag[Double] => implicitly[Numeric[Double]]
    case x if x == typeTag[Float]  => implicitly[Numeric[Float]]
  }).asInstanceOf[Numeric[NumType]]

  def parseExpression[A: TypeTag](e: Expression): A = (e match {
    // Literals
    case FieldIdent(_, _, _) | IntLiteral(_) | DateLiteral(_) | FloatLiteral(_) =>
      implicit val numVal = getNumVal(e.tp)
      parseNumericExpression(e)
    // Arithmetic Operators
    case Add(left, right) =>
      implicit val numVal = getNumVal(left.tp)
      parseNumericExpression(left) + parseNumericExpression(right)
    case Subtract(left, right) =>
      implicit val numVal = getNumVal(left.tp)
      parseNumericExpression(left) - parseNumericExpression(right)
    // Logical Operators
    case And(left, right) =>
      parseExpression[Boolean](left) && parseExpression[Boolean](right)
    // Comparison Operators
    /* case GreaterOrEqual(left, right) =>
      System.out.println(left.tp)
      System.out.println(right.tp)
      /*implicit val numVal = ({
        if (left.tp == typeTag[Double] && right.tp == typeTag[Float]) implicitly[Numeric[Float]]
        else {
          getNumVal(right.tp)
          //throw new Exception("Unsupported combination!")
        }
      }).asInstanceOf[Numeric[NumType]]
      //implicit val numVal = getNumVal(right.tp)*/
      val num1 = getNumVal(left.tp)
      val num2 = getNumVal(right.tp)

      parseNumericExpression(left)(num1, left.tp /*.asInstanceOf[TypeTag[NumType]]*/ ) >= parseNumericExpression(right)(num2, right.tp /*.asInstanceOf[TypeTag[NumType]]*/ )
    case LessThan(left, right) =>
      implicit val numVal = getNumVal(right.tp)
      parseNumericExpression(left) < parseNumericExpression(right)*/
  }).asInstanceOf[A]

  def parseWhereClauses(e: Option[Expression], parentOp: Operator[_]): Operator[_] = e match {
    case Some(expr) =>
      new SelectOp(parentOp)(arg => {
        currBindVar = arg.asInstanceOf[CaseClassRecord];
        parseExpression(expr).asInstanceOf[Boolean]
      })
    case None => parentOp
  }

  def convertOperators(sqlTree: SelectStatement): Operator[_] = {
    val scanOps = tableMap.map({ case (tableName, tab) => (tableName, new ScanOp(tab)) })
    val scanOp = scanOps.find(s => s._1 == "LINEITEM").get._2 // TODO: Generalize
    val selectOp = parseWhereClauses(sqlTree.where, scanOp)
    new PrintOp(selectOp)(t => {} /*println(t)*/ , -1)
  }

  def convert(sqlTree: SelectStatement) {
    val tableMap = loadRelations(sqlTree)
    val operatorTree = convertOperators(sqlTree)
    // Execute tree
    for (i <- 0 until Config.numRuns) {
      Utilities.time({
        operatorTree.open
        operatorTree.next
      }, "Query")
    }
  }
}
