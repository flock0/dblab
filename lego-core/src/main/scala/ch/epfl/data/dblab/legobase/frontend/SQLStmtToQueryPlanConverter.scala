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
import java.lang.reflect.Field
import tpch._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

class SQLTreeToQueryPlanConverter(schema: Schema) {
  val tableMap = new HashMap[String, Array[_]]()
  var currBindVar: CaseClassRecord = _

  case class IntermediateRecord2[A: TypeTag, B: TypeTag](arg1: A, arg2: B)(names: Seq[String]) extends CaseClassRecord {
    def getNameIndex(name: String) = names.indexOf(name) match {
      case 0 => "arg1"
      case 1 => "arg2"
    }
  }

  def loadRelations(sqlTree: SelectStatement) {
    val sqlRelations = sqlTree.relations;
    val schemaRelations = sqlRelations.map(r => schema.findTable(r match {
      case t: SQLTable => t.name
      case s: Subquery => s.alias
    }))
    schemaRelations.foreach(tn => tableMap += tn.name -> {
      // TODO: Generalize
      Loader.loadTable[LINEITEMRecord](tn)(classTag[LINEITEMRecord])
    })
  }

  // TODO Maybe move this to pardis as well?
  def recursiveGetField(rec: CaseClassRecord, name: String) {
    if (rec.getClass != classOf[LINEITEMRecord])
      System.out.println("looking for " + name + " in " + rec + " with currBindVar " + currBindVar)
    val fields = rec.getClass.getDeclaredFields
    fields.foreach(f => {
      f.setAccessible(true)
      f match {
        case c if f.getName == name => currBindVar = rec.asInstanceOf[CaseClassRecord] // SOURCE RELATIONS
        case c if f.get(rec).isInstanceOf[CaseClassRecord] =>
          //recursiveGetField(f.get(rec).asInstanceOf[CaseClassRecord], name)
          currBindVar = f.get(rec).asInstanceOf[CaseClassRecord]

        //case c if f.get(rec).isInstanceOf[Test2[_,_]] =>
        //currBindVar = rec.asInstanceOf[CaseClassRecord] // TODO -- FOO
        case _ =>
        //case _ => { if (rec.getClass != classOf[LINEITEMRecord]) System.out.println("ME HERE!" + currBindVar + " / " + rec); currBindVar = rec.asInstanceOf[CaseClassRecord] } //TODO FIX
      }
    })
    //
    //
    //  Some(fld.get(fld))
  }

  // TODO -- Maybe move a refactored version of the following function to CaseClassRecord class of SC?
  def printCaseClassRecord(ccr: CaseClassRecord) {
    def printMembers(v: Any, cls: Class[_]) {
      v match {
        case rec if rec.isInstanceOf[CaseClassRecord] =>
          printCaseClassRecord(rec.asInstanceOf[CaseClassRecord])
        case c if cls.isArray =>
          val arr = c.asInstanceOf[Array[_]]
          for (arrElem <- arr)
            printMembers(arrElem, arrElem.getClass)
        case c: Character => printf("%c|", c)
        case d: Double    => printf("%.2f|", d) // TODO -- Precision should not be hardcoded
        case _            =>
      }
    }
    val fields = ccr.getClass.getDeclaredFields
    fields.foreach(f => {
      f.setAccessible(true);
      val v = f.get(ccr);
      printMembers(v, v.getClass)
    })
  }

  def parseNumericExpression[A: TypeTag](e: Expression): A = (e match {
    case FieldIdent(_, name, _) => {
      recursiveGetField(currBindVar, name)
      if (currBindVar.getClass != classOf[LINEITEMRecord]) {
        System.out.println("RESULT IS  " + currBindVar)
        System.out.println(currBindVar.asInstanceOf[IntermediateRecord2[_, _]].getNameIndex(name))
      }
      // TODO -- 0?
      //fldList.find(fle => fle.getField(name).isDefined).get
      currBindVar match {
        case c if c.isInstanceOf[IntermediateRecord2[_, _]] => currBindVar.getField(currBindVar.asInstanceOf[IntermediateRecord2[_, _]].getNameIndex(name)).get
        case _ => currBindVar.getField(name).get
      }
    }
    case DateLiteral(v)  => v
    case FloatLiteral(v) => v
    case IntLiteral(v)   => v
    case _               => parseExpression(e)
  }).asInstanceOf[A]

  /**
   * This method receives two numbers and makes sure that both number have the same type.
   * If their type is different, it will upcast the number with lower type to the type
   * of the other number.
   */
  def promoteNumbers[A: TypeTag, B: TypeTag](n1: A, n2: B): (Any, Any) = {
    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    (
      (typeTag[A], typeTag[B]) match {
        case (x, y) if x == y        => n1 -> n2
        case (IntType, DoubleType)   => n1.asInstanceOf[Int].toDouble -> n2
        case (IntType, FloatType)    => n1.asInstanceOf[Int].toFloat -> n2
        case (FloatType, DoubleType) => n1.asInstanceOf[Float].toDouble -> n2
        case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
        case (DoubleType, IntType)   => n1 -> n2.asInstanceOf[Int].toDouble
        case (x, y)                  => throw new Exception(s"Does not know how to find the common type for $x and $y")
      }).asInstanceOf[(Any, Any)]
  }

  def computeOrderingExpression[A: TypeTag, B: TypeTag](e1: Expression, e2: Expression, op: (Ordering[Any]#Ops, Any) => Boolean): Boolean = {
    val n1 = parseNumericExpression[A](e1)
    val n2 = parseNumericExpression[B](e2)
    val (pn1, pn2) = promoteNumbers[A, B](n1, n2)
    val orderingA = getNumVal[A].asInstanceOf[Ordering[Any]]
    val opn1 = new orderingA.Ops(pn1)
    op(opn1, pn2)
  }

  def computeNumericExpression[A: TypeTag, B: TypeTag](e1: Expression, e2: Expression, op: (Numeric[Any]#Ops, Any) => Any): Any = {
    val n1 = parseNumericExpression[A](e1)
    val n2 = parseNumericExpression[B](e2)
    /*val (pn1, pn2) = promoteNumbers[A, B](n1, n2)
    val numericA = getNumVal[A].asInstanceOf[Numeric[Any]]
    val opn1 = new numericA.Ops(pn1)
    op(opn1, pn2)*/
    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    //System.out.println(typeTag[A])
    //System.out.println(typeTag[B])
    val (pn1, pn2) = (typeTag[A], typeTag[B]) match {
      case (x, y) if x == y      => n1 -> n2
      case (IntType, DoubleType) => n1.asInstanceOf[Int].toDouble -> n2
    }
    val numericDouble = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
    op(new numericDouble.Ops(pn1), pn2).asInstanceOf[Double]
  }

  def getNumVal[T](implicit tp: TypeTag[T]): Numeric[T] = (tp match {
    case x if x == typeTag[Int]    => implicitly[Numeric[Int]]
    case x if x == typeTag[Double] => implicitly[Numeric[Double]]
    case x if x == typeTag[Float]  => implicitly[Numeric[Float]]
    case dflt                      => throw new Exception("Type inferrence error -- Type " + dflt + " was not properly inferred during parsing!")
  }).asInstanceOf[Numeric[T]]

  def parseExpression[A: TypeTag](e: Expression): A = (e match {
    // Literals
    case FieldIdent(_, _, _) | IntLiteral(_) | DateLiteral(_) | FloatLiteral(_) =>
      parseNumericExpression(e)
    // Arithmetic Operators
    case Add(left, right) =>
      computeNumericExpression(left, right, (x, y) => x + y)(left.tp, right.tp)
    case Subtract(left, right) =>
      computeNumericExpression(left, right, (x, y) => x - y)(left.tp, right.tp)
    case Multiply(left, right) =>
      computeNumericExpression(left, right, (x, y) => x * y)(left.tp, right.tp)
    // Logical Operators
    case And(left, right) =>
      parseExpression[Boolean](left) && parseExpression[Boolean](right)
    case GreaterOrEqual(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x >= y)(left.tp, right.tp)
    case GreaterThan(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x > y)(left.tp, right.tp)
    case LessOrEqual(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x <= y)(left.tp, right.tp)
    case LessThan(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x < y)(left.tp, right.tp)
  }).asInstanceOf[A]

  def parseWhereClauses(e: Option[Expression], parentOp: Operator[_]): Operator[_] = e match {
    case Some(expr) =>
      new SelectOp(parentOp)(arg => {
        currBindVar = arg.asInstanceOf[CaseClassRecord]
        parseExpression(expr).asInstanceOf[Boolean]
      })
    case None => parentOp
  }

  def parseGroupBy(gb: Option[GroupBy]) = {
    System.out.println("Constructing group by...")
    gb match {
      case Some(GroupBy(listExpr, having)) =>
        val names = listExpr.map(le => le match {
          case FieldIdent(_, name, _) => name
        })
        (true, listExpr, names)
      case None => (false, null, Seq())
    }
  }

  def parseAggregations(e: Projections, gb: Option[GroupBy], parentOp: Operator[_]): Operator[_] = e match {
    case ExpressionProjections(proj) => {
      System.out.println("Constructing aggregations...")
      var (aggProj, gbProj) = proj.map(p => p._1).partition(p => !p.isInstanceOf[FieldIdent])

      val hasAVG = aggProj.exists(ap => ap.isInstanceOf[Avg])
      if (hasAVG && aggProj.indexOf(CountAll()) == -1)
        aggProj = aggProj :+ CountAll()

      val aggFuncs: Seq[(CaseClassRecord, Double) => Double] = aggProj.map(p => {
        (t: CaseClassRecord, currAgg: Double) =>
          currBindVar = t
          p match {
            case Sum(e)     => currAgg + parseExpression(e).asInstanceOf[Double]
            case Avg(e)     => currAgg + parseExpression(e).asInstanceOf[Double]
            case CountAll() => currAgg + 1
          }
      })

      val (hasIntermediateRecord, listExpr, names) = parseGroupBy(gb)

      val aggOp = new AggOp(parentOp.asInstanceOf[Operator[CaseClassRecord]], aggProj.length)(x => {
        currBindVar = x
        names.size match {
          case 0 => "Total"
          case 2 =>
            new IntermediateRecord2(parseExpression(listExpr(0)), parseExpression(listExpr(1)))(names)
        }
      })(aggFuncs: _*)

      if (hasAVG) {
        val countAllIdx = aggProj.indexOf(CountAll())
        val mapFuncs: Seq[CaseClassRecord => Unit] = aggProj.zipWithIndex.filter(ap => ap._1.isInstanceOf[Avg]).map(ap => {
          val (expr, idx) = (ap._1, ap._2);
          (t: CaseClassRecord) => {
            val arr = t.getField("aggs").get.asInstanceOf[Array[Double]]
            arr(idx) = arr(idx) / arr(countAllIdx)
          }
        })
        new MapOp(aggOp)(mapFuncs: _*);
      } else aggOp
    }
  }

  def parseOrderBy(ob: Option[OrderBy], parentOp: Operator[_]) = ob match {
    case Some(OrderBy(listExpr)) => {
      System.out.println("Constructing orderby...")
      // TODO GENERALIZE -- CaseClassRecord and Char types should not be below
      new SortOp(parentOp)((kv1, kv2) => {
        currBindVar = kv1.asInstanceOf[CaseClassRecord]
        val k1 = parseExpression(listExpr(0)._1).asInstanceOf[Char]
        currBindVar = kv2.asInstanceOf[CaseClassRecord]
        val k2 = parseExpression(listExpr(0)._1).asInstanceOf[Char]
        currBindVar = kv1.asInstanceOf[CaseClassRecord]
        val k3 = parseExpression(listExpr(1)._1).asInstanceOf[Char]
        currBindVar = kv2.asInstanceOf[CaseClassRecord]
        val k4 = parseExpression(listExpr(1)._1).asInstanceOf[Char]
        val res = k1 - k2
        if (res == 0) (k3 - k4).asInstanceOf[Int] else res.asInstanceOf[Int]
      })
    }
    case None => parentOp
  }

  def convertOperators(sqlTree: SelectStatement): Operator[_] = {
    val scanOps = tableMap.map({ case (tableName, tab) => (tableName, new ScanOp(tab)) })
    val scanOp = scanOps.find(s => s._1 == "LINEITEM").get._2 // TODO: Generalize
    val selectOp = parseWhereClauses(sqlTree.where, scanOp).asInstanceOf[SelectOp[LINEITEMRecord]] // TODO: Generalize
    val aggOp = parseAggregations(sqlTree.projections, sqlTree.groupBy, selectOp)
    val orderByOp = parseOrderBy(sqlTree.orderBy, aggOp)
    new PrintOp(orderByOp)(kv => {
      printCaseClassRecord(kv.asInstanceOf[CaseClassRecord])
      printf("\n")
    }, -1) // TODO -- Generalize limit
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