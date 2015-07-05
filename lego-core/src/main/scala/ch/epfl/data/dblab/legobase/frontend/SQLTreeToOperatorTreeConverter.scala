package ch.epfl.data
package dblab.legobase
package frontend

import schema._
import scala.reflect._
import OperatorAST._
import scala.reflect.runtime.{ universe => ru }
import ru._
import ch.epfl.data.dblab.legobase.queryengine.GenericEngine

class SQLTreeToOperatorTreeConverter(schema: Schema) {

  def createScanOperators(sqlTree: SelectStatement) = {
    sqlTree.relations.map(r => {
      val table = schema.findTable(r match {
        case t: SQLTable => t.name
      })
      ScanOpNode(table, table.name + r.asInstanceOf[SQLTable].alias.getOrElse(""), r.asInstanceOf[SQLTable].alias)
    })
  }

  def parseJoinAliases(leftParent: OperatorNode, rightParent: OperatorNode): (String, String) = {
    val leftAlias = leftParent match {
      case c if leftParent.isInstanceOf[ScanOpNode] => leftParent.asInstanceOf[ScanOpNode].qualifier
      case _                                        => Some("")
    }
    val rightAlias = rightParent match {
      case c if rightParent.isInstanceOf[ScanOpNode] => rightParent.asInstanceOf[ScanOpNode].qualifier
      case _                                         => Some("")
    }
    (leftAlias.getOrElse(""), rightAlias.getOrElse(""))
  }

  def parseJoinTree(e: Option[Relation], scanOps: Seq[ScanOpNode]): OperatorNode = e match {
    case None =>
      if (scanOps.size != 1)
        throw new Exception("Error in query: There are multiple input relations but no join! Cannot process such query statement!")
      else scanOps(0)
    case Some(joinTree) => joinTree match {
      case j: Join =>
        val leftOp = parseJoinTree(Some(j.left), scanOps)
        val rightOp = parseJoinTree(Some(j.right), scanOps)
        val (leftAlias, rightAlias) = parseJoinAliases(leftOp, rightOp)
        new JoinOpNode(leftOp, rightOp, j.clause, j.tpe, leftAlias, rightAlias)
      case r: SQLTable =>
        scanOps.find(so => so.scanOpName == r.name + r.alias.getOrElse("")) match {
          case Some(t) => t
          case None    => throw new Exception("LegoBase Frontend BUG: Table referenced in join but a Scan operator for this table was not constructed!")
        }
      case sq: Subquery => SubqueryNode(createMainOperatorTree(sq.subquery))
    }
  }

  def parseWhereClauses(e: Option[Expression], parentOp: OperatorNode): OperatorNode = e match {
    case Some(expr) => new SelectOpNode(parentOp, expr, false)
    case None       => parentOp
  }

  def getExpressionName(expr: Expression) = {
    expr match {
      case FieldIdent(qualifier, name, _) => qualifier.getOrElse("") + name
      case Year(_)                        => throw new Exception("When YEAR is used in group by it must be given an alias")
      case _                              => throw new Exception("Invalid Group by (Non-single attribute reference) expression " + expr + " found that does not appear in the select statement.")
    }
  }

  def parseGroupBy(gb: Option[GroupBy], proj: Seq[(Expression, Option[String])]) = gb match {
    case Some(GroupBy(exprList)) =>
      exprList.map(gbExpr => proj.find(p => p._1 == gbExpr) match {
        case Some(p) if p._2.isDefined  => (gbExpr, p._2.get)
        case Some(p) if !p._2.isDefined => (gbExpr, getExpressionName(gbExpr))
        case _ => proj.find(p => p._2 == Some(getExpressionName(gbExpr))) match {
          case Some(e) => (e._1, e._2.get)
          case None    => (gbExpr, getExpressionName(gbExpr))
        }
      })
    case None => Seq()
  }

  def parseAggregations(e: Projections, gb: Option[GroupBy], parentOp: OperatorNode): OperatorNode = e match {
    case ExpressionProjections(proj) => {

      val divisionIndexes = scala.collection.mutable.ArrayBuffer[(String, String)]()
      val hasDivide = proj.find(p => p._1.isInstanceOf[Divide]).isDefined

      val projsWithoutDiv = proj.map(p => (p._1 match {
        case Divide(e1, e2) =>
          val e1Name = p._2.getOrElse("")
          val e2Name = p._2.getOrElse("") + "_2"
          divisionIndexes += ((e1Name, e2Name))
          Seq((e1, Some(e1Name)), (e2, Some(e2Name)))
        case _ => Seq((p._1, p._2))
      })).flatten.asInstanceOf[Seq[(Expression, Option[String])]]

      var aggProjs = projsWithoutDiv.filter(p => p._1.isAggregateOpExpr)

      val hasAVG = aggProjs.exists(ap => ap._1.isInstanceOf[Avg])
      if (hasAVG && aggProjs.find(_._1.isInstanceOf[CountAll]) == None)
        aggProjs = aggProjs :+ (CountAll(), Some("__TOTAL_COUNT"))

      if (aggProjs == List()) parentOp
      else {
        val aggNames = aggProjs.map(agg => agg._2 match {
          case Some(al) => al
          case None     => throw new Exception("LegoBase Limitation: All aggregations must be given an alias (aggregation " + agg._1 + " was not)")
        })

        val aggOp = if (hasAVG) {
          val finalAggs = aggProjs.map(ag => ag._1 match {
            case Avg(e) => Sum(e)
            case _      => ag._1
          })

          //val countAllIdx = aggsAliases.indexOf(CountAll())
          val countAllName = aggProjs.find(_._1.isInstanceOf[CountAll]).get._2.get
          val mapIndices = aggProjs.filter(avg => avg._1.isInstanceOf[Avg]).map(avg => { (avg._2.get, countAllName) })
          MapOpNode(AggOpNode(parentOp, finalAggs, parseGroupBy(gb, proj), aggNames), mapIndices)
        } else AggOpNode(parentOp, aggProjs.map(_._1), parseGroupBy(gb, proj), aggNames)

        if (hasDivide) MapOpNode(aggOp, divisionIndexes)
        else aggOp
      }
    }
    case AllColumns() => parentOp
  }

  // TODO -- needs to be generalized and expanded with more cases
  def analyzeExprForSubquery(expr: Expression, parentOp: OperatorNode) = expr match {
    case GreaterThan(e, (sq: SelectStatement)) =>
      val rootOp = SubquerySingleResultNode(createMainOperatorTree(sq))
      val rhs = GetSingleResult(rootOp)
      rhs.setTp(typeTag[Double]) // FIXME
      SelectOpNode(parentOp, GreaterThan(e, rhs), true)
    case GreaterThan(e1, e2) => SelectOpNode(parentOp, GreaterThan(e1, e2), true)
    case LessThan(e, (sq: SelectStatement)) =>
      val rootOp = SubquerySingleResultNode(createMainOperatorTree(sq))
      val rhs = GetSingleResult(rootOp)
      rhs.setTp(typeTag[Double]) // FIXME
      SelectOpNode(parentOp, LessThan(e, rhs), true)
    case _ => parentOp
  }

  def parseHaving(having: Option[Having], parentOp: OperatorNode): OperatorNode = {
    having match {
      case Some(Having(expr)) =>
        analyzeExprForSubquery(expr, parentOp);
      case None => parentOp
    }
  }

  def parseOrderBy(ob: Option[OrderBy], parentOp: OperatorNode) = ob match {
    case Some(OrderBy(listExpr)) => OrderByNode(parentOp, listExpr)
    case None                    => parentOp
  }

  def createPrintOperator(parent: OperatorNode, e: Projections, limit: Option[Limit]) = {
    val projNames = e match {
      case ExpressionProjections(proj) => proj.map(p => p._2 match {
        case Some(al) => al
        case None => p._1 match {
          case FieldIdent(qualifier, name, _) => qualifier.getOrElse("") + name
          case _ => {
            if (!p._2.isDefined && p._1.isInstanceOf[Aggregation])
              throw new Exception("LegoBase limitation: Aggregates must always be aliased (e.g. SUM(...) AS TOTAL)")
            p._1.toString // TODO -- Not entirely correct if this is an aggregation without alias
          }
        }
      })
      case AllColumns() => Seq()
    }
    new PrintOpNode(parent, projNames, limit match {
      case Some(Limit(num)) => num.toInt
      case None             => -1
    })
  }

  def createMainOperatorTree(sqlTree: SelectStatement): OperatorNode = {
    val scanOps = createScanOperators(sqlTree)
    val hashJoinOp = parseJoinTree(sqlTree.joinTree, scanOps.toSeq)
    val selectOp = parseWhereClauses(sqlTree.where, hashJoinOp)
    val aggOp = parseAggregations(sqlTree.projections, sqlTree.groupBy, selectOp)
    val orderByOp = parseOrderBy(sqlTree.orderBy, aggOp)
    val havingOp = parseHaving(sqlTree.having, orderByOp)
    havingOp
  }

  def convert(sqlTree: SelectStatement) = {
    val topOperator = createMainOperatorTree(sqlTree)
    val printOp = createPrintOperator(topOperator, sqlTree.projections, sqlTree.limit)
    //System.out.println(printOp + "\n\n")
    printOp
  }
}
