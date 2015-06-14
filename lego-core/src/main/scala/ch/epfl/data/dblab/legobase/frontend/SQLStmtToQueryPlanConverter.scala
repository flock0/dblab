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
import sc.pardis.shallow.{ OptimalString, CaseClassRecord, DynamicCompositeRecord, Record }
import scala.language.implicitConversions

// TODO: The rest we should not need when we generalize
import java.lang.reflect.Field
import tpch._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import ch.epfl.data.dblab.legobase.queryengine.AGGRecord

class SQLTreeToQueryPlanConverter(schema: Schema) {
  val tableMap = new HashMap[String, Array[_]]()
  var aliasesList: Seq[ProjectionAlias] = Seq()
  var searchAliases: Boolean = false

  abstract class ProjectionAlias(val name: String)
  case class SimpleAlias(n: String, idx: Int) extends ProjectionAlias(n)
  case class CompositeAlias(n: String, idx: Int, arrayIdx: Int) extends ProjectionAlias(n)

  trait IntermediateRecord extends CaseClassRecord {
    def getNameIndex(name: String): Option[String]
  }

  case class IntermediateRecord2[A: TypeTag, B: TypeTag](arg1: A, arg2: B)(names: Seq[String]) extends IntermediateRecord {
    def getNameIndex(name: String) = names.indexOf(name) match {
      case 0 => Some("arg1")
      case 1 => Some("arg2")
      case _ => None
    }
  }

  case class IntermediateRecord3[A: TypeTag, B: TypeTag, C: TypeTag](arg1: A, arg2: B, arg3: C)(names: Seq[String]) extends IntermediateRecord {
    def getNameIndex(name: String) = names.indexOf(name) match {
      case 0 => Some("arg1")
      case 1 => Some("arg2")
      case 2 => Some("arg3")
      case _ => None
    }
  }

  case class IntermediateRecord7[A: TypeTag, B: TypeTag, C: TypeTag, D: TypeTag, E: TypeTag, F: TypeTag, G: TypeTag](arg1: A, arg2: B, arg3: C, args4: D, args5: E, args6: F, args7: G)(names: Seq[String]) extends IntermediateRecord {
    def getNameIndex(name: String) = names.indexOf(name) match {
      case 0 => Some("arg1")
      case 1 => Some("arg2")
      case 2 => Some("arg3")
      case 4 => Some("arg4")
      case 5 => Some("arg5")
      case 6 => Some("arg6")
      case 7 => Some("arg7")
      case _ => None
    }
  }

  // TODO Maybe move this to pardis as well?
  def recursiveGetField(name: String, t: Record, t2: Record = null) = {
    var stop = false
    var res: Record = null
    var alias: String = name

    def searchRecord(rec: Record): Record = {
      //System.out.println("Searching " + name + " in record " + rec)
      val fields = rec.getClass.getDeclaredFields

      // Maybe there is no need to go through the fields
      if (rec.isInstanceOf[DynamicCompositeRecord[_, _]]) {
        val cc = rec.asInstanceOf[DynamicCompositeRecord[_, _]]
        cc.getField(name) match {
          case Some(_) => stop = true; res = cc;
          case None    =>
        }
      }

      // Else exhaustively search inside the
      // TODO -- Quite imperative, maybe it can be turned to functional
      for (f <- fields if !stop) {
        f.setAccessible(true)
        f match {
          case c if f.getName == name =>
            stop = true; res = rec
          // TODO -- Generalize
          case c if f.get(rec).isInstanceOf[IntermediateRecord] =>
            val ir = f.get(rec).asInstanceOf[IntermediateRecord]
            ir.getNameIndex(name) match {
              case Some(al) => stop = true; res = ir; alias = al;
              case None     => searchRecord(ir)
            }
          case c if f.get(rec).isInstanceOf[DynamicCompositeRecord[_, _]] =>
            val cc = f.get(rec).asInstanceOf[DynamicCompositeRecord[_, _]]
            cc.getField(name) match {
              case Some(_) => stop = true; res = cc;
              case None    => searchRecord(cc)
            }
          case c if f.get(rec).isInstanceOf[Record] =>
            searchRecord(f.get(rec).asInstanceOf[Record])
          case _ =>
        }
      }
      res
    }

    val aliasedRec = aliasesList.find(al => al.name == name) match {
      case Some(al) if searchAliases => al match {
        case CompositeAlias(name, idx, arrayIdx) =>
          val f = t.getClass.getDeclaredFields()(idx)
          f.setAccessible(true)
          val e = f.get(t)
          e match {
            case r if r.isInstanceOf[IntermediateRecord] => {
              val rr = r.asInstanceOf[IntermediateRecord]
              rr.getField(rr.getNameIndex(name).get).get
            }
            case a if a.isInstanceOf[Array[_]] => a.asInstanceOf[Array[Double]](arrayIdx)
            case _                             => null
          }
        case SimpleAlias(name, idx) =>
          val f = t.getClass.getDeclaredFields()(idx)
          f.setAccessible(true)
          f.get(t)
      }
      case _ => null
    }
    if (aliasedRec != null) aliasedRec
    else {
      searchRecord(t)
      if (res == null) {
        if (t2 == null) throw new Exception("BUG: Searched for field " + name + " in " + t + " and couldn't find it! (and no other record available to search in)")
        searchRecord(t2)
        res match {
          case null =>
            throw new Exception("BUG: Searched for field " + name + " in " + t2 + " and couldn't find it! (and no other record available to search in)")
          case _ => res.getField(alias).get
        }
      } else res.getField(alias).get
    }
  }

  // TODO -- Maybe move a refactored version of the following function to Record class of SC?
  def printRecord(ccr: Record) {
    def printMembers(v: Any, cls: Class[_]) {
      v match {
        case rec if rec.isInstanceOf[Record] =>
          printRecord(rec.asInstanceOf[Record])
        case c if cls.isArray =>
          val arr = c.asInstanceOf[Array[_]]
          for (arrElem <- arr)
            printMembers(arrElem, arrElem.getClass)
        case i: Int           => printf("%d|", i)
        case c: Character     => printf("%c|", c)
        case d: Double        => printf("%.2f|", d)
        case str: String      => printf("%s|", str)
        case s: OptimalString => printf("%s|", s.string) // TODO -- Precision should not be hardcoded
        case _                => //throw new Exception("Do not know how to print member " + v + " of class " + cls)
      }
    }
    val fields = ccr.getClass.getDeclaredFields
    fields.foreach(f => {
      f.setAccessible(true);
      val v = f.get(ccr);
      printMembers(v, v.getClass)
    })
  }

  def parseNumericExpression[A: TypeTag](e: Expression, t: Record, t2: Record = null): A = (e match {
    case FieldIdent(_, name, _) => recursiveGetField(name, t, t2)
    case DateLiteral(v)         => v
    case FloatLiteral(v)        => v
    case DoubleLiteral(v)       => v
    case IntLiteral(v)          => v
    case StringLiteral(v)       => v
    case CharLiteral(v)         => v
    case _                      => parseExpression(e, t)
  }).asInstanceOf[A]

  /**
   * This method receives two numbers and makes sure that both number have the same type.
   * If their type is different, it will upcast the number with lower type to the type
   * of the other number.
   */
  /*def promoteNumbers[A: TypeTag, B: TypeTag](n1: A, n2: B): (Any, Any) = {
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
  }*/

  def computeOrderingExpression[A: TypeTag, B: TypeTag](e1: Expression, e2: Expression, op: (Ordering[Any]#Ops, Any) => Boolean, t: Record, t2: Record): Boolean = {
    val n1 = parseNumericExpression[A](e1, t, t2)
    val n2 = parseNumericExpression[B](e2, t, t2)
    /*val (pn1, pn2) = promoteNumbers[A, B](n1, n2)
    val orderingA = getNumVal[A].asInstanceOf[Ordering[Any]]
    val opn1 = new orderingA.Ops(pn1)
    op(opn1, pn2)*/
    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    //System.out.println(typeTag[A])
    //System.out.println(typeTag[B])
    val (pn1, pn2) = (typeTag[A], typeTag[B]) match {
      case (x, y) if x == y        => n1 -> n2
      case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
      case (DoubleType, IntType)   => n1 -> n2.asInstanceOf[Int].toDouble
      case (IntType, DoubleType)   => n1.asInstanceOf[Int].toDouble -> n2
    }

    val ordering = pn1.getClass match {
      case c if c == classOf[java.lang.Integer] || c == classOf[java.lang.Character] => implicitly[Numeric[Int]].asInstanceOf[Ordering[Any]]
      case d if d == classOf[java.lang.Double]                                       => implicitly[Numeric[Double]].asInstanceOf[Ordering[Any]]
    }
    op(new ordering.Ops(pn1), pn2) //.asInstanceOf[Double]
  }

  def performNumericComputation[A: TypeTag, B: TypeTag](n1: A, n2: B, op: (Numeric[Any]#Ops, Any) => Any): Any = {
    val IntType = typeTag[Int]
    val FloatType = typeTag[Float]
    val DoubleType = typeTag[Double]
    //System.out.println(typeTag[A])
    //System.out.println(typeTag[B])
    val (pn1, pn2) = (typeTag[A], typeTag[B]) match {
      case (x, y) if x == y        => n1 -> n2
      case (DoubleType, FloatType) => n1 -> n2.asInstanceOf[Float].toDouble
      case (DoubleType, IntType)   => n1 -> n2.asInstanceOf[Int].toDouble
      case (IntType, DoubleType)   => n1.asInstanceOf[Int].toDouble -> n2
    }

    val numeric = pn1.getClass match {
      case c if c == classOf[java.lang.Integer] || c == classOf[java.lang.Character] => implicitly[Numeric[Int]].asInstanceOf[Numeric[Any]]
      case d if d == classOf[java.lang.Double]                                       => implicitly[Numeric[Double]].asInstanceOf[Numeric[Any]]
    }
    op(new numeric.Ops(pn1), pn2) //.asInstanceOf[Double]
  }

  def computeNumericExpression[A: TypeTag, B: TypeTag](e1: Expression, e2: Expression, op: (Numeric[Any]#Ops, Any) => Any, t: Record, t2: Record = null): Any = {
    val n1 = parseNumericExpression[A](e1, t, t2)
    val n2 = parseNumericExpression[B](e2, t, t2)
    /*val (pn1, pn2) = promoteNumbers[A, B](n1, n2)
    val numericA = getNumVal[A].asInstanceOf[Numeric[Any]]
    val opn1 = new numericA.Ops(pn1)
    op(opn1, pn2)*/
    performNumericComputation(n1, n2, op)
  }

  /*def getNumVal[T](implicit tp: TypeTag[T]): Numeric[T] = (tp match {
    case x if x == typeTag[Int]    => implicitly[Numeric[Int]]
    case x if x == typeTag[Double] => implicitly[Numeric[Double]]
    case x if x == typeTag[Float]  => implicitly[Numeric[Float]]
    case dflt                      => throw new Exception("Type inferrence error -- Type " + dflt + " was not properly inferred during parsing!")
  }).asInstanceOf[Numeric[T]]*/

  def parseExpression[A: TypeTag](e: Expression, t: Record, t2: Record = null): A = (e match {
    // Literals
    case FieldIdent(_, _, _) | IntLiteral(_) | DateLiteral(_) | FloatLiteral(_) | StringLiteral(_) | CharLiteral(_) | DoubleLiteral(_) =>
      parseNumericExpression(e, t, t2)
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
    case Like(field, values, _) =>
      val f = parseExpression(field, t, t2)
      // not generic enough -- must generalize for other like styles (replacing %% not enough)
      // Also the replaceAll must go to the compiler
      val v = OptimalString(parseExpression(values, t, t2).asInstanceOf[OptimalString].string.replaceAll("%", "").getBytes)
      f.asInstanceOf[OptimalString].containsSlice(v.asInstanceOf[OptimalString])
    case Case(cond, thenp, elsep) =>
      val c = parseExpression(cond, t, t2)
      if (c == true) parseExpression(thenp, t, t2) else parseExpression(elsep, t, t2)
  }).asInstanceOf[A]

  def loadRelations(sqlTree: SelectStatement) {
    System.out.println("Constructing input relations...")
    val sqlRelations = sqlTree.relations;
    val schemaRelations = sqlRelations.foreach(r => {
      val table = schema.findTable(r match {
        case t: SQLTable => t.name
      })
      val tn = table
      val tableName = table.name + r.asInstanceOf[SQLTable].alias.getOrElse("")
      tableMap += (tableName -> (table.name match {
        // TODO: Generalize -- make this tpch agnostic
        case "LINEITEM" => Loader.loadTable[LINEITEMRecord](tn)(classTag[LINEITEMRecord])
        case "CUSTOMER" => Loader.loadTable[CUSTOMERRecord](tn)(classTag[CUSTOMERRecord])
        case "ORDERS"   => Loader.loadTable[ORDERSRecord](tn)(classTag[ORDERSRecord])
        case "REGION"   => Loader.loadTable[REGIONRecord](tn)(classTag[REGIONRecord])
        case "NATION"   => Loader.loadTable[NATIONRecord](tn)(classTag[NATIONRecord])
        case "SUPPLIER" => Loader.loadTable[SUPPLIERRecord](tn)(classTag[SUPPLIERRecord])
        case "PART"     => Loader.loadTable[PARTRecord](tn)(classTag[PARTRecord])
        case "PARTSUPP" => Loader.loadTable[PARTSUPPRecord](tn)(classTag[PARTSUPPRecord])
      }))
    })
  }

  def parseJoinClause(e: Expression): (Expression, Expression) = e match {
    case Equals(left, right) => (left, right)
    case And(left, equals)   => parseJoinClause(left)
  }

  def parseJoinTree(e: Option[Relation], scanOps: Seq[(String, Operator[_])]): Operator[_] = e match {
    case None =>
      if (scanOps.size != 1) throw new Exception("Error in query: There are multiple input relations but no join! Cannot process such query statement!")
      else scanOps(0)._2
    case Some(joinTree) => joinTree match {
      case j: Join =>
        val leftOp = parseJoinTree(Some(j.left), scanOps).asInstanceOf[Operator[Record]];
        val rightOp = parseJoinTree(Some(j.right), scanOps).asInstanceOf[Operator[Record]];
        val (leftCond, rightCond) = parseJoinClause(j.clause)
        j.tpe match {
          case LeftSemiJoin =>
            new LeftHashSemiJoinOp(leftOp, rightOp)((x, y) => parseExpression(j.clause, x, y).asInstanceOf[Boolean])(x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
          case _ =>
            new HashJoinOp(leftOp, rightOp, "N1_", "N2_")((x, y) => parseExpression(j.clause, x, y).asInstanceOf[Boolean])(x => parseExpression(leftCond, x)(leftCond.tp))(x => parseExpression(rightCond, x)(rightCond.tp))
        }
      case r: SQLTable => scanOps.find(so => so._1 == r.name + r.alias.getOrElse("")).get._2
    }
  }

  def parseWhereClauses(e: Option[Expression], parentOp: Operator[_]): Operator[_] = e match {
    case Some(expr) =>
      new SelectOp(parentOp.asInstanceOf[Operator[Record]])((t: Record) => {
        parseExpression(expr, t).asInstanceOf[Boolean]
      })
    case None => parentOp
  }

  def parseGroupBy(gb: Option[GroupBy], aliases: Seq[(Expression, String, Int)]) = {
    System.out.println("Constructing group by...")
    gb match {
      case Some(GroupBy(listExpr, having)) =>
        val names = listExpr.map(le => le._1 match {
          case FieldIdent(_, name, _) => name
          case Year(_) => le._2 match {
            case Some(q) => q
            case None    => throw new Exception("When YEAR is used in group by it must be given an alias")
          }
        })
        val finalListExpr = listExpr.map(le => le._1 match {
          case FieldIdent(_, name, _) =>
            aliases.find(e => e._2 == name) match {
              case Some(al) => al._1
              case None     => le._1
            }
          case Year(_) => le._1
        })
        (true, finalListExpr, names)
      case None => (false, null, Seq())
    }
  }

  def parseAggregations(e: Projections, gb: Option[GroupBy], parentOp: Operator[_], als: Seq[(Expression, String, Int)]): Operator[_] = e match {
    case ExpressionProjections(proj) => {
      System.out.println("Constructing aggregations...")
      var aliases = als
      var (aggProj, gbProj) = proj.map(p => p._1).partition(p => !p.isInstanceOf[FieldIdent] && !p.isInstanceOf[Year])

      val hasAVG = aggProj.exists(ap => ap.isInstanceOf[Avg])
      if (hasAVG && aggProj.indexOf(CountAll()) == -1)
        aggProj = aggProj :+ CountAll()

      val aggFuncs: Seq[(Record, Double) => Double] = aggProj.map(p => {
        (t: Record, currAgg: Double) =>
          p match {
            case Sum(e)     => performNumericComputation(currAgg, parseExpression(e, t), (x, y) => x + y)(typeTag[Double], e.tp).asInstanceOf[Double]
            case Avg(e)     => currAgg + parseExpression(e, t).asInstanceOf[Double]
            case CountAll() => currAgg + 1
          }
      })

      val (hasIntermediateRecord, listExpr, names) = parseGroupBy(gb, aliases)

      System.out.println("listExpr = " + listExpr)

      if (names.size == 1)
        aliases = aliases ++ Seq((listExpr(0), names(0), 0)) // for key

      val aggOp = new AggOp(parentOp.asInstanceOf[Operator[Record]], aggProj.length)((t: Record) => {
        names.size match {
          case 0 => "Total"
          case 1 =>
            parseExpression(listExpr(0), t)
          case 2 =>
            new IntermediateRecord2(parseExpression(listExpr(0), t), parseExpression(listExpr(1), t)(listExpr(1).tp))(names)
          case 3 =>
            new IntermediateRecord3(parseExpression(listExpr(0), t), parseExpression(listExpr(1), t), parseExpression(listExpr(2), t))(names)
          case 7 =>
            new IntermediateRecord7(parseExpression(listExpr(0), t), parseExpression(listExpr(1), t), parseExpression(listExpr(2), t),
              parseExpression(listExpr(3), t), parseExpression(listExpr(4), t), parseExpression(listExpr(5), t),
              parseExpression(listExpr(6), t))(names)
        }
      })(aggFuncs: _*)

      // Handle aliases, as they may be used in order by -- TODO -- CHECK, GENERALIZE AND IMPROVE 
      aliasesList = aliases.map(al => {
        val idx = al._3
        val isAggregation = proj(idx)._1.isInstanceOf[Aggregation]
        if (isAggregation)
          CompositeAlias(al._2, 1, aggProj.indexOf(al._1))
        else {
          if (gbProj.size == 1) SimpleAlias(al._2, 0)
          else CompositeAlias(al._2, 0, gbProj.indexOf(al._1))
        }
      })

      System.out.println("Aliases list is now: " + aliasesList)

      // Construct map op following an aggregation, in case there is an average
      if (hasAVG) {
        val countAllIdx = aggProj.indexOf(CountAll())
        val mapFuncs: Seq[Record => Unit] = aggProj.zipWithIndex.filter(ap => ap._1.isInstanceOf[Avg]).map(ap => {
          val (expr, idx) = (ap._1, ap._2);
          (t: Record) => {
            val arr = t.getField("aggs").get.asInstanceOf[Array[Double]]
            arr(idx) = arr(idx) / arr(countAllIdx)
          }
        })
        new MapOp(aggOp)(mapFuncs: _*);
      } else aggOp
    }
    case AllColumns() => parentOp
  }

  def parseOrderBy(ob: Option[OrderBy], parentOp: Operator[_]) = ob match {
    case Some(OrderBy(listExpr)) => {
      System.out.println("Constructing orderby...")
      new SortOp(parentOp.asInstanceOf[Operator[Record]])((kv1: Record, kv2: Record) => {
        searchAliases = true // Enable alias search so that aliases of projection can be referenced

        var stop = false;
        var res = 0;
        for (e <- listExpr if !stop) {
          val k1 = parseExpression(e._1, kv1)(e._1.tp)
          val k2 = parseExpression(e._1, kv2)(e._1.tp)
          // TODO:  there should be no reason to check for NULL -- type inference BUG for aggregations
          res = (e._1.tp match {
            case i if i == typeTag[Int]                 => k1.asInstanceOf[Int] - k2.asInstanceOf[Int]
            case d if d == null || d == typeTag[Double] => k1.asInstanceOf[Double] - k2.asInstanceOf[Double]
            case c if c == typeTag[Char]                => k1.asInstanceOf[Char] - k2.asInstanceOf[Char]
            case s if s == typeTag[VarCharType]         => k1.asInstanceOf[OptimalString].diff(k2.asInstanceOf[OptimalString])
          }).asInstanceOf[Int]
          if (res != 0) {
            stop = true
            if (e._2 == DESC) res = -res
          }
        }
        res
      })
    }
    case None => parentOp
  }

  def convertOperators(sqlTree: SelectStatement): Operator[_] = {
    val scanOps = tableMap.map({ case (tableName, tab) => (tableName, new ScanOp(tab)) })
    val hashJoinOp = parseJoinTree(sqlTree.joinTree, scanOps.toSeq)
    val selectOp = parseWhereClauses(sqlTree.where, hashJoinOp)
    val aggOp = parseAggregations(sqlTree.projections, sqlTree.groupBy, selectOp, sqlTree.aliases)
    val orderByOp = parseOrderBy(sqlTree.orderBy, aggOp)
    new PrintOp(orderByOp)(kv => {
      printRecord(kv.asInstanceOf[Record])
      printf("\n")
    }, sqlTree.limit match {
      case Some(Limit(num)) => num.toInt
      case None             => -1
    })
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