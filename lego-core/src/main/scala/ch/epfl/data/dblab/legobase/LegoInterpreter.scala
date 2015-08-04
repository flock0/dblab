package ch.epfl.data
package dblab.legobase

import utils._
import schema._
import frontend._
import ch.epfl.data.dblab.legobase.frontend.OperatorAST._
import storagemanager._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.collection.mutable.ArrayBuffer
import ch.epfl.data.dblab.legobase.queryengine.push._
import ch.epfl.data.dblab.legobase.queryengine.GenericEngine
import sc.pardis.shallow.{ OptimalString, Record, DynamicCompositeRecord }

/**
 * The main object for interpreting queries.
 */
object LegoInterpreter extends LegoRunner {
  def main(args: Array[String]) {
    // Some checks to avoid silly exceptions
    if (args.length < 4) {
      println("ERROR: Invalid number (" + args.length + ") of command line arguments!")
      println("USAGE: run <data_folder> <test_suite> <scaling_factor_number> <list of queries to run>")
      println("     : test_suite should be \"TPCH\" or \"TPCDS\"")
      println("     : data_folder should contain folders named sf0.1 sf1 sf2 sf4 etc")
      System.exit(0)
    }

    run(args)
  }

  /**
   * Interprets the given query for the given schema
   *
   * @param query the input operator tree
   * @param schema the input schema
   */
  def executeQuery(operatorTree: OperatorNode, schema: Schema): Unit = {
    val qp = convertOperator(operatorTree)
    // Execute tree
    for (i <- 0 until Config.numRuns) {
      Utilities.time({
        qp.open
        qp.next
      }, "Query")
    }
  }

  // Query interpretation methods
  val decimalFormatter = new java.text.DecimalFormat("###.########");

  def recursiveGetField(name: String, t: Record, t2: Record = null): Any = {
    var stop = false

    def searchRecordFields(rec: LegobaseRecord): Option[Any] = {
      var res: Option[Any] = None
      for (f <- rec.getNestedRecords() if !stop) {
        searchRecord(f) match {
          case Some(r) => res = Some(r); stop = true;
          case None    =>
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
          case None    => None //searchRecordFields(rec)
        }
      } else searchRecordFields(rec.asInstanceOf[LegobaseRecord])
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
    order.size match {
      case 0 =>
        val fieldNames = rec.asInstanceOf[LegobaseRecord].getFieldNames
        fieldNames.foreach(fn => {
          val f = recursiveGetField(fn, rec)
          printMembers(f, f.getClass)
        })
      case _ =>
        order.foreach(n => {
          val f = recursiveGetField(n, rec)
          printMembers(f, f.getClass)
        })
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
    pn1.getClass match {
      case c if c == classOf[java.lang.Integer] || c == classOf[java.lang.Character] =>
        val numeric = implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]
        op(new numeric.Ops(pn1), pn2)
      case d if d == classOf[java.lang.Double] =>
        val numeric = implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
        decimalFormatter.format(op(new numeric.Ops(pn1), pn2)).toDouble
    }
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
    case Substring(field, idx, len) =>
      val f = parseExpression(field, t, t2).asInstanceOf[OptimalString]
      val from = parseExpression(idx, t, t2).asInstanceOf[Int] - 1
      val until = from + parseExpression(len, t, t2).asInstanceOf[Int]
      f.slice(from, until)
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
      val rec = p.getResult.asInstanceOf[LegobaseRecord]
      //if (rec.numFields > 1) throw new Exception("LegoBase BUG: Do not know how to extract single value from record with > 1 fields (" + rec.numFields + " fields found -- rec = " + rec + ").")
      rec.getField(rec.getFieldNames().toList.last).get // TODO: Generalize, this is a hack
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
        //TODO Generalize!
        val rightTp = Manifest.classType((rightParent match {
          case ScanOpNode(_, _, _)                     => classTag[LegobaseRecord]
          case SelectOpNode(ScanOpNode(_, _, _), _, _) => classTag[LegobaseRecord]
        }).runtimeClass).asInstanceOf[Manifest[Record]]
        new LeftOuterJoinOp(leftOp, rightOp)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))(rightTp)
      case AntiJoin =>
        new HashJoinAnti(leftOp, rightOp)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
      case InnerJoin =>
        new HashJoinOp(leftOp, rightOp, leftAlias, rightAlias)((x, y) => parseExpression(joinCond, x, y).asInstanceOf[Boolean])(
          x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
    }
  }

  def createAggOpOperator(parentOp: OperatorNode, aggs: Seq[Expression], gb: Seq[(Expression, String)], aggNames: Seq[String]): Operator[_] = {
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

    val keyName = gb.size match {
      case 0 => None
      case 1 => Some(gb(0)._2)
      case _ => Some("key")
    }

    val grp: (Record) => Any = (t: Record) => {
      gb.size match {
        case 0 => Seq(StringLiteral(GenericEngine.parseString("NO_GROUP_BY"))) zip Seq("NO_GROUP_BY")
        case 1 => parseExpression(gb(0)._1, t)
        case _ =>
          val keyFieldAttrNames = gb.map(_._2)
          val keyExpr = gb.map(_._1).map(gbExpr => parseExpression(gbExpr, t)(gbExpr.tp))
          new LegobaseRecord(keyFieldAttrNames zip keyExpr)
      }
    }

    new AggOp(convertOperator(parentOp).asInstanceOf[Operator[Record]], aggs.length)(grp, keyName)(aggFuncs: _*)(aggNames)
  }

  def createSelectOperator(parentOp: OperatorNode, cond: Expression): Operator[_] = {
    new SelectOp(convertOperator(parentOp).asInstanceOf[Operator[Record]])((t: Record) => {
      parseExpression(cond, t).asInstanceOf[Boolean]
    })
  }

  def createMapOperator(parentOp: OperatorNode, indices: Seq[(String, String)]): Operator[_] = {
    val mapFuncs: Seq[Record => Unit] = indices.map(idx => idx match {
      case (idx1, idx2) => (t: Record) => {
        val record = t.asInstanceOf[LegobaseRecord]
        record.setField(idx1, record.getField(idx1).get.asInstanceOf[Double] / record.getField(idx2).get.asInstanceOf[Double])
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
    }, limit)
  }

  def convertOperator(node: OperatorNode): Operator[_] = node match {
    case ScanOpNode(table, _, _) =>
      new ScanOp(Loader.loadTable(table))
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
}
