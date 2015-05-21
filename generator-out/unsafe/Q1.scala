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


case class SortOp_Any(val sortedTree: TreeSet[AGGRecord_GroupByClass], val expectedSize: Int, var stop: Boolean)
case class LINEITEMRecord(val L_QUANTITY: Double, val L_EXTENDEDPRICE: Double, val L_DISCOUNT: Double, val L_TAX: Double, val L_RETURNFLAG: Char, val L_LINESTATUS: Char, val L_SHIPDATE: Int)
case class GroupByClass(val L_RETURNFLAG: Char, val L_LINESTATUS: Char)
case class AGGRecord_GroupByClass(val key: GroupByClass, val aggs: Array[Double])
object Q1 extends LegoRunner {
  def executeQuery(query: String, sf: Double, schema: ch.epfl.data.dblab.legobase.schema.Schema): Unit = main()
  def main(args: Array[String]) {
    run(args)
  }
  def main() = 
  {
    val x1 = Loader.fileLineCount("/home/florian/Documents/tpch_testdata/sf0.1/lineitem.tbl")
    val x2 = new Array[LINEITEMRecord](x1)
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
      val x46 = LINEITEMRecord(x13, x14, x15, x16, x17, x18, x19)
      val x47 = x4
      val x48 = x2.update(x47, x46)
      val x49 = x4
      val x50 = x49.+(1)
      val x51 = x4 = x50
      x51
    }
    val x53 = new Range(0, 1, 1)
    val x321 = { x54: Int => {
        val x439 = new HashMap[GroupByClass, AGGRecord_GroupByClass]()
        val x320 = GenericEngine.runQuery({
          val x55 = GenericEngine.parseDate("1998-09-02")
          var x381: Int = 0
          val x147 = { (x131: AGGRecord_GroupByClass, x132: AGGRecord_GroupByClass) => {
              val x133 = x131.key
              val x134 = x133.L_RETURNFLAG
              val x135 = x132.key
              val x136 = x135.L_RETURNFLAG
              val x137 = x134.-(x136)
              var res138: Int = x137
              val x139 = res138
              val x140 = x139.==(0)
              val x145 = if(x140) 
              {
                val x141 = x133.L_LINESTATUS
                val x142 = x135.L_LINESTATUS
                val x143 = x141.-(x142)
                val x144 = res138 = x143
                x144
              }
              else
              {
                ()
              }
              
              val x146 = res138
              x146
            }
          }
          val x486 = OrderingFactory(x147)
          val x487 = new TreeSet()(x486)
          val x148 = SortOp_Any(x487, 1024, false)
          var x508: Int = 0
          val x262 = while({
            val x170 = true.&&({
              val x1202 = x381
              val x169 = x1202.<(x1)
              x169
            })
            x170
          })
          {
            val x1204 = x381
            val x172 = x2.apply(x1204)
            val x174 = x172.L_SHIPDATE
            val x175 = x174.<=(x55)
            val x258 = if(x175) 
            {
              val x176 = x172.L_RETURNFLAG
              val x177 = x172.L_LINESTATUS
              val x178 = GroupByClass(x176, x177)
              val x182 = x439.getOrElseUpdate(x178, {
                val x180 = new Array[Double](9)
                val x181 = AGGRecord_GroupByClass(x178, x180)
                x181
              })
              val x183 = x182.aggs
              val x197 = x183.apply(0)
              val x198 = x172.L_DISCOUNT
              val x199 = x198.+(x197)
              val x200 = x183.update(0, x199)
              val x207 = x183.apply(1)
              val x208 = x172.L_QUANTITY
              val x209 = x208.+(x207)
              val x210 = x183.update(1, x209)
              val x217 = x183.apply(2)
              val x218 = x172.L_EXTENDEDPRICE
              val x219 = x218.+(x217)
              val x220 = x183.update(2, x219)
              val x227 = x183.apply(3)
              val x228 = 1.0.-(x198)
              val x229 = x218.*(x228)
              val x230 = x229.+(x227)
              val x231 = x183.update(3, x230)
              val x238 = x183.apply(4)
              val x239 = x172.L_TAX
              val x240 = 1.0.+(x239)
              val x241 = x229.*(x240)
              val x242 = x241.+(x238)
              val x243 = x183.update(4, x242)
              val x250 = x183.apply(5)
              val x252 = x250.+(1.0)
              val x253 = x183.update(5, x252)
              ()
            }
            else
            {
              ()
            }
            
            val x1291 = x381
            val x260 = x1291.+(1)
            val x1293 = x381 = x260
            x1293
          }
          val x289 = { x264: Tuple2[GroupByClass, AGGRecord_GroupByClass] => {
              val x265 = x264._2
              val x269 = x265.aggs
              val x270 = x269.apply(1)
              val x271 = x269.apply(5)
              val x272 = x270./(x271)
              val x273 = x269.update(6, x272)
              val x275 = x269.apply(2)
              val x276 = x269.apply(5)
              val x277 = x275./(x276)
              val x278 = x269.update(7, x277)
              val x280 = x269.apply(0)
              val x281 = x269.apply(5)
              val x282 = x280./(x281)
              val x283 = x269.update(8, x282)
              val x285 = x148.!=(null)
              val x288 = if(x285) 
              {
                val x286 = x148.sortedTree
                val x287 = x286.+=(x265)
                ()
              }
              else
              {
                ()
              }
              
              x288
            }
          }
          val x290 = x439.foreach(x289)
          val x317 = while({
            val x291 = x148.stop
            val x292 = x291.unary_!
            val x296 = x292.&&({
              val x293 = x148.sortedTree
              val x294 = x293.size
              val x295 = x294.!=(0)
              x295
            })
            x296
          })
          {
            val x297 = x148.sortedTree
            val x298 = x297.head
            val x299 = x297.-=(x298)
            val x301 = x298.key
            val x302 = x301.L_RETURNFLAG
            val x303 = x301.L_LINESTATUS
            val x304 = x298.aggs
            val x305 = x304.apply(1)
            val x306 = x304.apply(2)
            val x307 = x304.apply(3)
            val x308 = x304.apply(4)
            val x309 = x304.apply(6)
            val x310 = x304.apply(7)
            val x311 = x304.apply(8)
            val x312 = x304.apply(5)
            val x313 = printf("%c|%c|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.0f\n", x302, x303, x305, x306, x307, x308, x309, x310, x311, x312)
            val x1345 = x508
            val x315 = x1345.+(1)
            val x1347 = x508 = x315
            x1347
          }
          val x1348 = x508
          val x319 = printf("(%d rows)\n", x1348)
          ()
        })
        x320
      }
    }
    val x322 = x53.foreach(x321)
    x322
  }
}