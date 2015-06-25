package ch.epfl.data
package dblab.legobase
package frontend

import schema._
import tpch._ // TODO -- Will die
import scala.reflect._
import OperatorAST._
import scala.reflect.runtime.{ universe => ru }
import ru._

class SQLTreeToOperatorTreeConverter(schema: Schema) {

  def createScanOperators(sqlTree: SelectStatement) = {
    sqlTree.relations.map(r => {
      val table = schema.findTable(r match {
        case t: SQLTable => t.name
      })
      ScanOpNode(table, table.name + r.asInstanceOf[SQLTable].alias.getOrElse(""), r.asInstanceOf[SQLTable].alias, table.name match {
        // TODO: Generalize -- make this tpch agnostic
        case "LINEITEM" => classTag[LINEITEMRecord]
        case "CUSTOMER" => classTag[CUSTOMERRecord]
        case "ORDERS"   => classTag[ORDERSRecord]
        case "REGION"   => classTag[REGIONRecord]
        case "NATION"   => classTag[NATIONRecord]
        case "SUPPLIER" => classTag[SUPPLIERRecord]
        case "PART"     => classTag[PARTRecord]
        case "PARTSUPP" => classTag[PARTSUPPRecord]
      })
    })
  }

  def parseJoinAliases(leftParent: OperatorNode, rightParent: OperatorNode): (String, String) = {
    val leftAlias = leftParent match {
      case c if leftParent.isInstanceOf[ScanOpNode[_]] => leftParent.asInstanceOf[ScanOpNode[_]].qualifier
      case _ => Some("")
    }
    val rightAlias = rightParent match {
      case c if rightParent.isInstanceOf[ScanOpNode[_]] => rightParent.asInstanceOf[ScanOpNode[_]].qualifier
      case _ => Some("")
    }
    (leftAlias.getOrElse(""), rightAlias.getOrElse(""))
  }

  def parseJoinTree(e: Option[Relation], scanOps: Seq[ScanOpNode[_]]): OperatorNode = e match {
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

  def parseAggregations(e: Projections, gb: Option[GroupBy], parentOp: OperatorNode): OperatorNode = e match {
    case ExpressionProjections(proj) => {

      val divisionAliases = scala.collection.mutable.HashMap[String, AggregateDivisionAlias]()

      var idx = 0;
      val projsWithoutDiv = proj.map(p => (p._1 match {
        case Divide(e1, e2) =>
          val aliasName = p._2.getOrElse("DIVISION")
          val e1Name = Some(p._2.getOrElse("") + "_1")
          val e2Name = Some(p._2.getOrElse("") + "_2")
          divisionAliases += aliasName -> AggregateDivisionAlias(aliasName, idx, idx + 1)
          idx += 2
          Seq((e1, e2Name), (e2, e2Name))
        case _ =>
          if (!p._1.isInstanceOf[FieldIdent] && !p._1.isInstanceOf[Year]) idx += 1
          Seq((p._1, p._2))
      })).flatten.asInstanceOf[Seq[(Expression, Option[String])]]

      val aggProjs = projsWithoutDiv.filter(p => !p._1.isInstanceOf[FieldIdent] && !p._1.isInstanceOf[Year])
      var aggs = aggProjs.map(p => p._1)

      val hasAVG = aggs.exists(ap => ap.isInstanceOf[Avg])
      if (hasAVG && aggs.indexOf(CountAll()) == -1)
        aggs = aggs :+ CountAll()

      if (aggs == List()) parentOp
      else {
        val aggAlias = aggProjs.map(p => p._2).zipWithIndex.filter(p => p._1.isDefined).map(p => AggregateValueAlias(p._1.get, p._2)) ++
          divisionAliases.values.toSeq

        //System.out.println(aggAlias)

        if (hasAVG) {
          val finalAggs = aggs.map(ag => ag match {
            case Avg(e) => Sum(e)
            case _      => ag
          })

          val countAllIdx = aggs.indexOf(CountAll())
          val mapIndices = aggs.zipWithIndex.filter(avg => avg._1.isInstanceOf[Avg]).map(avg => { (avg._2, countAllIdx) })
          MapOpNode(AggOpNode(parentOp, finalAggs, gb, aggAlias), mapIndices)
        } else AggOpNode(parentOp, aggs, gb, aggAlias)
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