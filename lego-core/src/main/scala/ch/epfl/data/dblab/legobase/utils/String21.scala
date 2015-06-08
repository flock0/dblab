package ch.epfl.data
package dblab.legobase
package utils

import ch.epfl.data.sc.pardis.annotations._
import offheap._

@deep
@noImplementation
@data class String21(
  val __0: Byte,
  val __1: Byte,
  val __2: Byte,
  val __3: Byte,
  val __4: Byte,
  val __5: Byte,
  val __6: Byte,
  val __7: Byte,
  val __8: Byte,
  val __9: Byte,
  val __10: Byte,
  val __11: Byte,
  val __12: Byte,
  val __13: Byte,
  val __14: Byte,
  val __15: Byte,
  val __16: Byte,
  val __17: Byte,
  val __18: Byte,
  val __19: Byte,
  val __20: Byte,
  val len: Byte) {

  def iterator = new Iterator[Byte] {
    var currentElement = 0
    def hasNext = currentElement < len
    def next = {
      if (!hasNext)
        Iterator.empty.next
      val b = apply(currentElement)
      currentElement += 1
      b
    }
  }

  def ===(o: String21): Boolean = this.iterator.sameElements(o.iterator)
  def =!=(o: String21): Boolean = !(===(o))

  def apply(i: Int): Byte =
    if (i >= len)
      throw new NoSuchElementException(s"no element with index $i")
    else i match {
      case 0  => __0
      case 1  => __1
      case 2  => __2
      case 3  => __3
      case 4  => __4
      case 5  => __5
      case 6  => __6
      case 7  => __7
      case 8  => __8
      case 9  => __9
      case 10 => __10
      case 11 => __11
      case 12 => __12
      case 13 => __13
      case 14 => __14
      case 15 => __15
      case 16 => __16
      case 17 => __17
      case 18 => __18
      case 19 => __19
      case 20 => __20
      case _  => throw new NoSuchElementException(s"no element with index $i")
    }

  def string: String = {
    val sb = new StringBuilder()
    this.iterator.foreach { b => sb += b.toChar }
    sb.toString
  }
}

object String21 {
  def apply(data: scala.Array[Byte], offset: Int, length: Byte)(implicit alloc: Allocator): String21 = {
    if (length > 21)
      throw new IllegalArgumentException("String21 can only take up to 21 bytes")
    val trimmed = data.drop(offset).take(length)
    val filled = if (length < 21)
      trimmed ++ scala.Array.fill(21 - length)(0: Byte)
    else
      trimmed
    String21(filled(0), filled(1), filled(2), filled(3), filled(4), filled(5),
      filled(6), filled(7), filled(8), filled(9), filled(10), filled(11),
      filled(12), filled(13), filled(14), filled(15), filled(16), filled(17),
      filled(18), filled(19), filled(20), length)
  }
  def default(implicit alloc: Allocator) = apply("".getBytes, 0, 0)
}