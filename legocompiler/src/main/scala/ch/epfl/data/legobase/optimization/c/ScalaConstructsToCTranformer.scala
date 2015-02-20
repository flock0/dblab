package ch.epfl.data
package legobase
package optimization
package c

import deep._
import pardis.optimization._
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.types._

class ScalaConstructsToCTranformer(override val IR: LoweringLegoBase, val ifAgg: Boolean) extends RecursiveRuleBasedTransformer[LoweringLegoBase](IR) with CTransformer {
  import IR._
  import CNodes._
  import CTypes._

  def blockIsPure[T](block: Block[T]): Boolean = {
    for (stm <- block.stmts) {
      val isPure = stm.rhs match {
        case b @ Block(_, _) => blockIsPure(b)
        case d               => d.isPure
      }
      if (!isPure)
        return false
    }
    true
  }

  def isNullCheck[T](exp: Rep[T]): Boolean = exp match {
    case Def(NotEqual(_, Constant(null))) => true
    case Def(Equal(_, Constant(null)))    => true
    case _                                => false
  }

  rewrite += rule {
    case IfThenElse(cond, thenp, elsep) if thenp.tp != UnitType =>
      val res = __newVarNamed[Int](unit(0), "ite")(thenp.tp.asInstanceOf[TypeRep[Int]])
      __ifThenElse(cond, {
        __assign(res, inlineBlock(thenp))
      }, {
        __assign(res, inlineBlock(elsep))
      })
      ReadVar(res)(res.tp)
  }

  rewrite += rule {
    case and @ Boolean$bar$bar(case1, b) if ifAgg && blockIsPure(b) && !isNullCheck(case1) => {
      val resB = inlineBlock[Boolean](b)
      // NameAlias[Boolean](Some(case1), " or ", List(List(resB)))
      case1 | resB
    }
  }

  rewrite += rule {
    case and @ Boolean$amp$amp(case1, b) if ifAgg && blockIsPure(b) && !isNullCheck(case1) => {
      val resB = inlineBlock[Boolean](b)
      // NameAlias[Boolean](Some(case1), " and ", List(List(resB)))
      case1 & resB
    }
  }

  rewrite += rule {
    case or @ Boolean$bar$bar(case1, b) => {
      __ifThenElse(case1, unit(true), b)
    }
  }

  rewrite += rule {
    case and @ Boolean$amp$amp(case1, b) => {
      __ifThenElse(case1, b, unit(false))
    }
  }
  // rewrite += rule {
  //   case and @ Boolean$amp$amp(case1, b) if b.stmts.forall(stm => stm.rhs.isPure) && b.stmts.nonEmpty => {
  //     val rb = inlineBlock(b)
  //     case1 && rb
  //   }
  // }
  // rewrite += rule {
  //   case and @ Boolean$amp$amp(case1, b) if b.stmts.nonEmpty => {
  //     __ifThenElse(case1, b, unit(false))
  //   }
  // }
  rewrite += rule { case IntUnary_$minus(self) => unit(-1) * self }
  rewrite += rule { case IntToLong(x) => x }
  rewrite += rule { case ByteToInt(x) => x }
  rewrite += rule { case IntToDouble(x) => x }
  rewrite += rule { case DoubleToInt(x) => infix_asInstanceOf[Double](x) }
  rewrite += rule { case BooleanUnary_$bang(b) => NameAlias[Boolean](None, "!", List(List(b))) }
}