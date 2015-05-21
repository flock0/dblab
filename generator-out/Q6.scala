package ch.epfl.data
package dblab.legobase


import scala.collection.mutable.Set
import scala.collection.mutable.HashMap
import scala.collection.mutable.TreeSet
import scala.collection.mutable.ArrayBuffer
import storagemanager.K2DBScanner
import storagemanager.Loader
import queryengine.GenericEngine
import sc.pardis.shallow.OptimalString
import sc.pardis.shallow.scalalib.collection.Cont

class MultiMap[T, S] extends HashMap[T, Set[S]] with scala.collection.mutable.MultiMap[T, S]

object OrderingFactory {
  def apply[T](fun: (T, T) => Int): Ordering[T] = new Ordering[T] {
    def compare(o1: T, o2: T) = fun(o1, o2)
  }
}


case class AGGRecord_String(val key: String, val aggs: Array[Double])
case class LINEITEMRecord(val L_QUANTITY: Double, val L_EXTENDEDPRICE: Double, val L_DISCOUNT: Double, val L_SHIPDATE: Int)
object Q6 extends LegoRunner {
  def executeQuery(query: String, sf: Double, schema: ch.epfl.data.dblab.legobase.schema.Schema): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  {
    val x1 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/lineitem.tbl")
    val x2 = new UnsafeArray[LINEITEMRecord](classOf[LINEITEMRecord], x1)
    val x3 = new K2DBScanner("/home/florian/Documents/tpch_testdata/sf0.1/lineitem.tbl")
    var x4: Int = 0
    val x52 = while({
      val x5 = x4
      val x6 = x5.<(x1)
      val x8 = x6.&&({
        val x7 = x3.hasNext()
        x7
      })
      x8
    })
    {
      val x9 = x3.next_int()
      val x10 = x3.next_int()
      val x11 = x3.next_int()
      val x12 = x3.next_int()
      val x13 = x3.next_double()
      val x14 = x3.next_double()
      val x15 = x3.next_double()
      val x16 = x3.next_double()
      val x17 = x3.next_char()
      val x18 = x3.next_char()
      val x19 = x3.next_date
      val x20 = x3.next_date
      val x21 = x3.next_date
      val x23 = new Array[Byte](26)
      val x24 = x3.next(x23)
      val x27 = { x25: Byte => {
          val x26 = x25.!=(0)
          x26
        }
      }
      val x28 = x23.filter(x27)
      val x29 = new OptimalString(x28)
      val x31 = new Array[Byte](11)
      val x32 = x3.next(x31)
      val x35 = { x33: Byte => {
          val x34 = x33.!=(0)
          x34
        }
      }
      val x36 = x31.filter(x35)
      val x37 = new OptimalString(x36)
      val x39 = new Array[Byte](45)
      val x40 = x3.next(x39)
      val x43 = { x41: Byte => {
          val x42 = x41.!=(0)
          x42
        }
      }
      val x44 = x39.filter(x43)
      val x45 = new OptimalString(x44)
      val x46 = LINEITEMRecord(x13, x14, x15, x19)
      val x47 = x4
      val x48 = x2.copyAndSet(x46, x47)
      val x49 = x4
      val x50 = x49.+(1)
      val x51 = x4 = x50
      x51
    }
    val x53 = new Range(0, 1, 1)
    val x162 = { x54: Int => {
        val x1070 = new Array[Double](1)
        val x1071 = AGGRecord_String("Total", x1070)
        val x161 = GenericEngine.runQuery({
          val x55 = GenericEngine.parseDate("1996-01-01")
          val x56 = GenericEngine.parseDate("1997-01-01")
          var x223: Int = 0
          var x269: Int = 0
          val x146 = while({
            val x97 = true.&&({
              val x631 = x223
              val x96 = x631.<(x1)
              x96
            })
            x97
          })
          {
            val x633 = x223
            val x99 = x2.get(x633)
            val x101 = x99.L_SHIPDATE
            val x102 = x101.>=(x55)
            val x113 = x102.&&({
              val x103 = x101.<(x56)
              val x112 = x103.&&({
                val x104 = x99.L_DISCOUNT
                val x105 = x104.>=(0.08)
                val x111 = x105.&&({
                  val x106 = x104.<=(0.1)
                  val x110 = x106.&&({
                    val x107 = x99.L_QUANTITY
                    val x109 = x107.<(24.0)
                    x109
                  })
                  x110
                })
                x111
              })
              x112
            })
            val x142 = if(x113) 
            {
              val x118 = x1071.aggs
              val x132 = x118.apply(0)
              val x133 = x99.L_EXTENDEDPRICE
              val x134 = x99.L_DISCOUNT
              val x135 = x133.*(x134)
              val x136 = x135.+(x132)
              val x137 = x118.update(0, x136)
              ()
            }
            else
            {
              ()
            }
            
            val x677 = x223
            val x144 = x677.+(1)
            val x679 = x223 = x144
            x679
          }
          val x1115 = Tuple2.apply(null, x1071)
          val x1116 = x1115._2
          val x1117 = x1116.aggs
          val x1118 = x1117.apply(0)
          val x1119 = printf("%.4f\n", x1118)
          val x1120 = x269
          val x1121 = x1120.+(1)
          val x1122 = x269 = x1121
          val x691 = x269
          val x160 = printf("(%d rows)\n", x691)
          ()
        })
        x161
      }
    }
    val x163 = x53.foreach(x162)
    x163
  }
}