package ch.epfl.data
package dblab.legobase
package frontend

import utils._
import schema._
import OperatorAST._
import storagemanager._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.dblab.legobase.queryengine.push._
import ch.epfl.data.dblab.legobase.queryengine.AGGRecord
import ch.epfl.data.dblab.legobase.queryengine.GenericEngine
import sc.pardis.shallow.{ OptimalString, Record, DynamicCompositeRecord }

class SQLTreeToQueryPlanConverter(schema: Schema) {

  var aggregateAliasesList: Seq[AggregateAlias] = Seq()

  // TODO -- Maybe move a refactored version of the following function to Record class of SC?
  def printRecord(rec: Record, order: Seq[String] = Seq()) {
    //System.out.println("Printing record...")
    def printMembers(v: Any, cls: Class[_]) {
      v match {
        case rec if rec.isInstanceOf[Record] =>
          printRecord(rec.asInstanceOf[Record], order)
        case c if cls.isArray =>
          val arr = c.asInstanceOf[Array[_]]
          for (arrElem <- arr)
            printMembers(arrElem, arrElem.getClass)
        case i: Int           => printf("%d|", i)
        case c: Character     => printf("%c|", c)
        case d: Double        => printf("%.2f|", d) // TODO -- Precision should not be hardcoded
        case str: String      => printf("%s|", str)
        case s: OptimalString => printf("%s|", s.string)
        case _                => //throw new Exception("Do not know how to print member " + v + " of class " + cls)
      }
    }
    val fields = rec.getClass.getDeclaredFields
    order.size match {
      case 0 =>
        fields.foreach(f => {
          f.setAccessible(true);
          val v = f.get(rec);
          printMembers(v, v.getClass)
        })
      case _ =>
        order.foreach(n => {
          val f = recursiveGetField(n, rec)
          printMembers(f, f.getClass)
        })
    }
  }

  def recursiveGetField(name: String, t: Record, t2: Record = null): Any = {
    var stop = false
    //var alias: String = name

    def searchRecordFields(rec: Record): Option[Any] = {
      val fields = rec.getClass.getDeclaredFields
      var res: Option[Any] = None
      for (f <- fields if !stop) {
        f.setAccessible(true)
        f match {
          case c if f.get(rec).isInstanceOf[IntermediateRecord] =>
            val ir = f.get(rec).asInstanceOf[IntermediateRecord]
            ir.getNameIndex(name) match {
              case Some(al) =>
                res = Some(ir.getField(al).get); stop = true
              case None =>
            }
          case c if f.get(rec).isInstanceOf[AGGRecord[_]] =>
            val r = f.get(rec).asInstanceOf[AGGRecord[_]]
            System.out.println(aggregateAliasesList)
            aggregateAliasesList.find(al => al.name == name) match {
              case Some(AggregateKeyAlias(_))                 => res = Some(r.key); stop = true;
              case Some(AggregateValueAlias(_, idx))          => res = Some(r.aggs(idx)); stop = true;
              case Some(AggregateDivisionAlias(_, idx, idx2)) => res = Some(r.aggs(idx) / r.aggs(idx2)); stop = true;
              case None                                       => searchRecordFields(r.key.asInstanceOf[Record])
            }
          case c if f.get(rec).isInstanceOf[Record] =>
            searchRecord(f.get(rec).asInstanceOf[Record])
          case _ => /* NOT_FOUND, PROCEED WITH NEXT ELEM */
        }
      }
      res
    }

    def searchRecord(rec: Record): Option[Any] = {
      /* Does a field exist with this name? If so immediately return it */
      val res = rec.getField(name)

      if (res.isDefined) res
      /* If not, is it a dynamic record on which we can look (and find) this attribute name?*/
      else if (rec.isInstanceOf[DynamicCompositeRecord[_, _]]) {
        val r = rec.asInstanceOf[DynamicCompositeRecord[_, _]]
        r.getField(name) match {
          case Some(f) => Some(f)
          case None    => searchRecordFields(rec)
        }
      } else if (rec.isInstanceOf[AGGRecord[_]]) {
        val r = rec.asInstanceOf[AGGRecord[_]]
        //System.out.println(aggregateAliasesList)
        aggregateAliasesList.find(al => al.name == name) match {
          case Some(AggregateKeyAlias(_))                 => Some(r.key)
          case Some(AggregateValueAlias(_, idx))          => Some(r.aggs(idx))
          case Some(AggregateDivisionAlias(_, idx, idx2)) => Some(r.aggs(idx) / r.aggs(idx2))
          case None                                       => searchRecordFields(rec)
        }
      } else None /* Else we haven't found it */
    }

    searchRecord(t) match {
      case Some(res) => res // rec.getField(name).get
      case None =>
        if (t2 == null) throw new Exception("BUG: Searched for field " + name + " in " + t + " and couldn't find it! (and no other record available to search in)")
        searchRecord(t2) match {
          case None =>
            throw new Exception("BUG: Searched for field " + name + " in " + t + " and couldn't find it! (and no other record available to search in)")
          case Some(res) => res //rec.getField(name).get
        }
    }
  }

  /**
   * This method receives two numbers and makes sure that both number have the same type.
   * If their type is different, it will upcast the number with lower type to the type
   * of the other number.
   */
  def promoteNumbers[A: TypeTag, B: TypeTag](n1: A, n2: B) = {

    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    // Check that we have inferred typetags correctly
    //if (typeTag[A] == null || typeTag[B] == null)
    //throw new Exception("LegoBase type inferrence BUG: inferred types should never be NULL (detected types (" + typeTag[A] + "," + typeTag[B] + ")")

    ((typeTag[A], typeTag[B]) match {
      case (x, y) if x == y      => n1 -> n2
      case (IntType, DoubleType) => n1.asInstanceOf[Int].toDouble -> n2
      //case (IntType, FloatType)    => n1.asInstanceOf[Int].toFloat -> n2
      //case (FloatType, DoubleType) => n1.asInstanceOf[Float].toDouble -> n2
      //case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
      case (DoubleType, IntType) => n1 -> n2.asInstanceOf[Int].toDouble
      case _                     => n1.asInstanceOf[Double].toDouble -> n2.asInstanceOf[Double].toDouble // FIXME FIXME FIXME THIS SHOULD BE HAPPENING, TYPE INFERENCE BUG
      //case (x, y)                  => throw new Exception(s"Does not know how to find the common type for $x and $y")
    }).asInstanceOf[(Any, Any)]
  }

  def computeOrderingExpression[A: TypeTag, B: TypeTag](e1: Expression, e2: Expression, op: (Ordering[Any]#Ops, Any) => Boolean, t: Record, t2: Record = null): Boolean = {
    val n1 = parseExpression[A](e1, t, t2)
    val n2 = parseExpression[B](e2, t, t2)
    val (pn1, pn2) = promoteNumbers(n1, n2)
    val ordering = pn1.getClass match {
      case c if c == classOf[java.lang.Integer] || c == classOf[java.lang.Character] => implicitly[Numeric[Int]].asInstanceOf[Ordering[Any]]
      case d if d == classOf[java.lang.Double]                                       => implicitly[Numeric[Double]].asInstanceOf[Ordering[Any]]
    }
    op(new ordering.Ops(pn1), pn2)
  }

  def computeNumericExpression[A: TypeTag, B: TypeTag](e1: Expression, e2: Expression, op: (Numeric[Any]#Ops, Any) => Any, t: Record, t2: Record = null): Any = {
    val n1 = parseExpression[A](e1, t, t2)
    val n2 = parseExpression[B](e2, t, t2)
    val (pn1, pn2) = promoteNumbers(n1, n2)
    val numeric = pn1.getClass match {
      case c if c == classOf[java.lang.Integer] || c == classOf[java.lang.Character] => implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]
      case d if d == classOf[java.lang.Double]                                       => implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
    }
    op(new numeric.Ops(pn1), pn2)
  }

  val subqueryInitializedMap = new scala.collection.mutable.HashMap[OperatorNode, SubquerySingleResult[_]]()
  def parseExpression[A: TypeTag](e: Expression, t: Record, t2: Record = null): A = (e match {
    // Literals
    case FieldIdent(qualifier, name, _) => recursiveGetField(qualifier.getOrElse("") + name, t, t2)
    case DateLiteral(v)                 => v
    case FloatLiteral(v)                => v
    case DoubleLiteral(v)               => v
    case IntLiteral(v)                  => v
    case StringLiteral(v)               => v
    case CharLiteral(v)                 => v
    // Arithmetic Operators
    case Add(left, right) =>
      computeNumericExpression(left, right, (x, y) => x + y, t, t2)(left.tp, right.tp)
    case Subtract(left, right) =>
      computeNumericExpression(left, right, (x, y) => x - y, t, t2)(left.tp, right.tp)
    case Multiply(left, right) =>
      computeNumericExpression(left, right, (x, y) => x * y, t, t2)(left.tp, right.tp)
    // Logical Operators
    case Equals(left, right) =>
      parseExpression(left, t, t2)(left.tp) == parseExpression(right, t, t2)(right.tp)
    case NotEquals(left, right) =>
      parseExpression(left, t, t2)(left.tp) != parseExpression(right, t, t2)(right.tp)
    case And(left, right) =>
      parseExpression[Boolean](left, t, t2) && parseExpression[Boolean](right, t, t2)
    case Or(left, right) =>
      parseExpression[Boolean](left, t, t2) || parseExpression[Boolean](right, t, t2)
    case GreaterOrEqual(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x >= y, t, t2)(left.tp, right.tp)
    case GreaterThan(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x > y, t, t2)(left.tp, right.tp)
    case LessOrEqual(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x <= y, t, t2)(left.tp, right.tp)
    case LessThan(left, right) =>
      computeOrderingExpression(left, right, (x, y) => x < y, t, t2)(left.tp, right.tp)
    // SQL statements
    case Year(date) =>
      val d = parseExpression(date, t, t2)
      d.asInstanceOf[Int] / 10000;
    case Substring(field, idx1, idx2) =>
      val f = parseExpression(field, t, t2).asInstanceOf[OptimalString]
      val index1 = parseExpression(idx1, t, t2).asInstanceOf[Int] - 1
      val index2 = parseExpression(idx2, t, t2).asInstanceOf[Int]
      f.slice(index1, index2)
    case Like(field, value, negate) =>
      val f = parseExpression(field, t, t2).asInstanceOf[OptimalString]
      val s = parseExpression(value, t, t2).asInstanceOf[OptimalString]
      val str = s.string
      val delim = "%%"
      // TODO: In what follows, the replaceAll call must go to the compiler
      val res = str match {
        case c if str.startsWith(delim) && str.endsWith(delim) && delim.r.findAllMatchIn(str).length == 2 =>
          val v = OptimalString(str.replaceAll("%", "").getBytes)
          f.containsSlice(v)
        case c if str.startsWith(delim) && str.endsWith(delim) && delim.r.findAllMatchIn(str).length == 3 =>
          val substrings = str.replaceFirst(delim, "").split(delim).map(GenericEngine.parseString(_))
          val idxu = f.indexOfSlice(substrings(0), 0)
          val idxp = f.indexOfSlice(substrings(1), idxu)
          idxu != -1 && idxp != -1
        case c if str.endsWith(delim) =>
          val v = OptimalString(str.replaceAll("%", "").getBytes)
          f.startsWith(v)
        case c if str.startsWith(delim) =>
          val v = OptimalString(str.replaceAll("%", "").getBytes)
          f.endsWith(v)
      }
      if (negate) !res else res
    case Case(cond, thenp, elsep) =>
      val c = parseExpression(cond, t, t2)
      if (c == true) parseExpression(thenp, t, t2) else parseExpression(elsep, t, t2)
    // TODO -- Not good. Must be generalized. The extraction of the single field should go earlier in the pipeline.
    case GetSingleResult(parent) =>
      val p = subqueryInitializedMap.getOrElseUpdate(parent, convertOperator(parent).asInstanceOf[SubquerySingleResult[_]])
      p.getResult match {
        case c if c.isInstanceOf[AGGRecord[_]] =>
          val aggRec = c.asInstanceOf[AGGRecord[_]]
          aggRec.aggs.size match {
            case 1 => aggRec.aggs(0)
            case _ => aggRec.aggs(0) // TODO -- Generalize (see Q22
          }
        case dflt => throw new Exception("LegoBase BUG: Do not know how to extract single value from record " + dflt)
      }
  }).asInstanceOf[A]

  // TODO -- Generalize! 
  def parseJoinClause(e: Expression): (Expression, Expression) = e match {
    case Equals(left, right) => (left, right)
    case And(left, equals)   => parseJoinClause(left)
  }

  def createJoinOperator(leftParent: OperatorNode, rightParent: OperatorNode, joinCond: Expression, joinType: JoinType, leftAlias: String, rightAlias: String): Operator[_] = {
    val leftOp = convertOperator(leftParent).asInstanceOf[Operator[Record]]
    val rightOp = convertOperator(rightParent).asInstanceOf[Operator[Record]]
    val (leftCond, rightCond) = parseJoinClause(joinCond)
    joinType match {
      case LeftSemiJoin =>
        new LeftHashSemiJoinOp(leftOp, rightOp)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
      case LeftOuterJoin =>
        new LeftOuterJoinOp(leftOp, rightOp)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
      case AntiJoin =>
        new HashJoinAnti(leftOp, rightOp)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
      case InnerJoin =>
        new HashJoinOp(leftOp, rightOp, leftAlias, rightAlias)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
    }
  }

  def parseGroupBy(gb: Option[GroupBy]) = gb match {
    case Some(GroupBy(exprList)) =>
      val ids = (exprList.map(e => e._2 match {
        case Some(q) => q
        case None => e._1 match {
          case FieldIdent(qualifier, name, _) => qualifier.getOrElse("") + name
          case Year(_)                        => throw new Exception("When YEAR is used in group by it must be given an alias")
        }
      })).toSeq
      val finalExpr = exprList.map(_._1).toSeq
      (finalExpr, ids)
    case None => (Seq(StringLiteral(GenericEngine.parseString("Total"))), Seq("Total"))
  }

  def createAggOpOperator(parentOp: OperatorNode, aggs: Seq[Expression], gb: Option[GroupBy], aggAliases: Seq[AggregateAlias]): Operator[_] = {
    val aggFuncs: Seq[(Record, Double) => Double] = aggs.map(p => {
      (t: Record, currAgg: Double) =>
        p match {
          case Sum(e) => computeNumericExpression(DoubleLiteral(currAgg), e, (x, y) => x + y, t)(typeTag[Double], e.tp).asInstanceOf[Double]
          case Min(e) =>
            val newMin = parseExpression(e, t)(e.tp).asInstanceOf[Double]
            if (currAgg == 0 || newMin < currAgg) newMin // TODO -- Assumes that 0 cannot occur naturally in the data as a min value. FIXME
            else currAgg
          case CountAll() => currAgg + 1
          case CountExpr(expr) => {
            // From http://www.techonthenet.com/sql/count.php: "Not everyone realizes this, but the SQL COUNT function will only include the 
            // records in the count where the value of expression in COUNT(expression) is NOT NULL". 
            // Here we use pardis default value to test for the above condition
            import sc.pardis.shallow.utils.DefaultValue
            if (parseExpression(expr, t)(expr.tp) != DefaultValue(expr.tp.tpe.dealias.toString)) currAgg + 1
            else currAgg
          }
          case IntLiteral(v)    => v
          case DoubleLiteral(v) => v
        }
    })

    val (gbExprs, gbIDs) = parseGroupBy(gb)

    aggregateAliasesList = aggregateAliasesList ++ aggAliases

    if (gbIDs.size == 1) // for key, in the case key is a single elemenent (otherwise we resolve inside the struct -- see note in OperatorAST)
      aggregateAliasesList = aggregateAliasesList ++ Seq(AggregateKeyAlias(gbIDs(0)))

    //System.out.println("AggregateOp DEBUG info: numAggs = " + aggs.length + "\n\t gbExprs=" + gbExprs + "\n\t gbIDs  = " + gbIDs + " \n\t aggregateAliasesList = " + aggregateAliasesList)

    new AggOp(convertOperator(parentOp).asInstanceOf[Operator[Record]], aggs.length)((t: Record) => {
      val listExpr = gbExprs.map(e => parseExpression(e, t)(e.tp))
      gbIDs.size match {
        case 1 => listExpr(0)
        case 2 => new IntermediateRecord2(listExpr(0), listExpr(1))(gbIDs)
        case 3 => new IntermediateRecord3(listExpr(0), listExpr(1), listExpr(2))(gbIDs)
        case 4 => new IntermediateRecord4(listExpr(0), listExpr(1), listExpr(2), listExpr(3))(gbIDs)
        case 5 => new IntermediateRecord5(listExpr(0), listExpr(1), listExpr(2), listExpr(3), listExpr(4))(gbIDs)
        case 7 => new IntermediateRecord7(listExpr(0), listExpr(1), listExpr(2),
          listExpr(3), listExpr(4), listExpr(5), listExpr(6))(gbIDs)
      }
    })(aggFuncs: _*)
  }

  def createSelectOperator(parentOp: OperatorNode, cond: Expression): Operator[_] = {
    new SelectOp(convertOperator(parentOp).asInstanceOf[Operator[Record]])((t: Record) => {
      parseExpression(cond, t).asInstanceOf[Boolean]
    })
  }

  def createMapOperator(parentOp: OperatorNode, indices: Seq[(Int, Int)]): Operator[_] = {
    val mapFuncs: Seq[Record => Unit] = indices.map(idx => idx match {
      case (idx1, idx2) => (t: Record) => {
        val arr = t.getField("aggs").get.asInstanceOf[Array[Double]]
        arr(idx1) = arr(idx1) / arr(idx2)
      }
    })
    new MapOp(convertOperator(parentOp).asInstanceOf[Operator[Record]])(mapFuncs: _*);
  }

  def createSortOperator(parentOp: OperatorNode, orderBy: Seq[(Expression, OrderType)]) = {
    new SortOp(convertOperator(parentOp).asInstanceOf[Operator[Record]])((kv1: Record, kv2: Record) => {
      var stop = false;
      var res = 0;
      for (e <- orderBy if !stop) {
        val k1 = parseExpression(e._1, kv1)(e._1.tp)
        val k2 = parseExpression(e._1, kv2)(e._1.tp)
        res = (e._1.tp match {
          case i if i == typeTag[Int]         => k1.asInstanceOf[Int] - k2.asInstanceOf[Int]
          // Multiply with 100 to account for very small differences (e.g. 0.02)
          case d if d == typeTag[Double]      => (k1.asInstanceOf[Double] - k2.asInstanceOf[Double]) * 100
          case c if c == typeTag[Char]        => k1.asInstanceOf[Char] - k2.asInstanceOf[Char]
          case s if s == typeTag[VarCharType] => k1.asInstanceOf[OptimalString].diff(k2.asInstanceOf[OptimalString])
          case _                              => (k1.asInstanceOf[Double] - k2.asInstanceOf[Double]) * 100 // TODO -- Type inference bug -- there should be absolutely no need for that and this will soon DIE
        }).asInstanceOf[Int]
        if (res != 0) {
          stop = true
          if (e._2 == DESC) res = -res
        }
      }
      res
    })
  }

  def createPrintOperator(parent: OperatorNode, projNames: Seq[String], limit: Int) = {
    new PrintOp(convertOperator(parent))(kv => {
      projNames.size match {
        case 0 => printRecord(kv.asInstanceOf[Record])
        case _ => printRecord(kv.asInstanceOf[Record], projNames)
      }
      printf("\n")
    }, limit)
  }

  def convertOperator(node: OperatorNode): Operator[_] = node match {
    case ScanOpNode(table, _, _, tp) =>
      new ScanOp(Loader.loadTable(table)(tp))
    case SelectOpNode(parent, cond, _) =>
      createSelectOperator(parent, cond)
    case JoinOpNode(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias) =>
      createJoinOperator(leftParent, rightParent, joinCond, joinType, leftAlias, rightAlias)
    case AggOpNode(parent, aggs, gb, aggAlias) =>
      createAggOpOperator(parent, aggs, gb, aggAlias)
    case MapOpNode(parent, indices) =>
      createMapOperator(parent, indices)
    case OrderByNode(parent, orderBy) =>
      createSortOperator(parent, orderBy)
    case PrintOpNode(parent, projNames, limit) =>
      createPrintOperator(parent, projNames, limit)
    case SubqueryNode(parent) => convertOperator(parent)
    case SubquerySingleResultNode(parent) =>
      new SubquerySingleResult(convertOperator(parent))
  }

  def convert(sqlTree: OperatorNode) {
    val operatorTree = convertOperator(sqlTree)
    // Execute tree
    for (i <- 0 until Config.numRuns) {
      Utilities.time({
        operatorTree.open
        operatorTree.next
      }, "Query")
    }
  }
}
