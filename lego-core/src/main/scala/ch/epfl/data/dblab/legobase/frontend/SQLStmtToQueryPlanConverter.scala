package ch.epfl.data
package dblab.legobase
package frontend

import utils._
import Numeric.Implicits._
import ch.epfl.data.dblab.legobase.queryengine.GenericEngine
import schema._
import storagemanager._
import ch.epfl.data.dblab.legobase.queryengine.push._
import scala.collection.mutable.HashMap
import sc.pardis.shallow.CaseClassRecord

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

  def parseNumericExpression[A: Numeric: TypeTag](e: Expression): A = (e match {
    case FieldIdent(_, name, _) =>
      currBindVar.getField(name).get
    case DateLiteral(v) => v
  }).asInstanceOf[A]

  def typeTagToNumeric[A: TypeTag]: Numeric[A] = (typeTag[A] match {
    case x if x == typeTag[Int] => implicitly[Numeric[Int]]
  }).asInstanceOf[Numeric[A]]

  def parseExpression[A: TypeTag](e: Expression): A = (e match {
    case GreaterOrEqual(left, right) =>
      class NumType
      assert(left.tp == right.tp)
      implicit val typeNum = left.tp.asInstanceOf[TypeTag[NumType]]
      implicit val numericNum = typeTagToNumeric(typeNum)
      parseNumericExpression[NumType](left) gteq
        parseNumericExpression[NumType](right)
    case DateLiteral(v) => GenericEngine.parseDate(v)
    case FieldIdent(_, name, _) =>
      currBindVar.getField(name).get
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
