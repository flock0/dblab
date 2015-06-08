package ch.epfl.data
package dblab.legobase
package utils

import sc.pardis.shallow.OptimalString
import ch.epfl.data.sc.pardis.annotations._
import offheap._

@deep
@noImplementation
@needs[(Tuple2[_, _], Array[_])]
@data class TupleString(
  @embed val __0: String21,
  @embed val __1: String21,
  @embed val __2: String21,
  @embed val __3: String21,
  @embed val __4: String21,
  @embed val __5: String21,
  @embed val __6: String21,
  @embed val __7: String21,
  @embed val __8: String21,
  @embed val __9: String21,
  val length: Int) {

  def iterator = __0.iterator ++
    __1.iterator ++ __2.iterator ++ __3.iterator ++
    __4.iterator ++ __5.iterator ++ __6.iterator ++
    __7.iterator ++ __8.iterator ++ __9.iterator

  def apply(i: Int): Byte = ???
  def startsWith(o: TupleString): Boolean = ???
  def containsSlice(o: TupleString): Boolean = ???
  def slice(start: Int, end: Int): TupleString = ???
  def indexOfSlice(o: TupleString, i: Int): Int = ???
  def endsWith(that: TupleString): Boolean = this.iterator.drop(length - that.length).sameElements(that.iterator)

  def diff(that: TupleString): Int = (this.iterator zip that.iterator).foldLeft(0)((res, e) => { if (res == 0) e._1 - e._2 else res })
  def ===(that: TupleString): Boolean = this.iterator.sameElements(that.iterator)
  def =!=(that: TupleString): Boolean = !(===(that))
  def zip(o: TupleString) = ???
  def foldLeft(c: Int)(f: (Int, Byte) => Int): Int = ???
  def reverse: TupleString = ???
  def split(delimiter: Array[Char]): Array[TupleString] = ???
  def string: String = {
    val sb = new StringBuilder()
    this.iterator.foreach { b => sb += b.toChar }
    sb.toString
  }
}
object TupleString {
  def apply(data: scala.Array[Byte])(implicit alloc: Allocator): TupleString = {
    if (data.length > 210)
      throw new IllegalArgumentException("data is longer than 210 characters")
    var remaining = data.length

    val __0 = if (remaining <= 0) {
      String21(data, 0 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 0 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 0 * 21, 21)
      remaining -= 21
      str
    }
    val __1 = if (remaining <= 0) {
      String21(data, 1 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 1 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 1 * 21, 21)
      remaining -= 21
      str
    }
    val __2 = if (remaining <= 0) {
      String21(data, 2 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 2 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 2 * 21, 21)
      remaining -= 21
      str
    }
    val __3 = if (remaining <= 0) {
      String21(data, 3 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 3 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 3 * 21, 21)
      remaining -= 21
      str
    }
    val __4 = if (remaining <= 0) {
      String21(data, 4 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 4 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 4 * 21, 21)
      remaining -= 21
      str
    }
    val __5 = if (remaining <= 0) {
      String21(data, 5 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 5 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 5 * 21, 21)
      remaining -= 21
      str
    }
    val __6 = if (remaining <= 0) {
      String21(data, 6 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 6 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 6 * 21, 21)
      remaining -= 21
      str
    }
    val __7 = if (remaining <= 0) {
      String21(data, 7 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 7 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 7 * 21, 21)
      remaining -= 21
      str
    }
    val __8 = if (remaining <= 0) {
      String21(data, 8 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 8 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 8 * 21, 21)
      remaining -= 21
      str
    }
    val __9 = if (remaining <= 0) {
      String21(data, 9 * 21, 0)
    } else if (remaining <= 21) {
      val str = String21(data, 9 * 21, remaining.toByte)
      remaining = 0
      str
    } else {
      val str = String21(data, 9 * 21, 21)
      remaining -= 21
      str
    }
    TupleString(__0, __1, __2, __3, __4, __5, __6, __7, __8, __9,
      __0.len + __1.len + __2.len +
        __3.len + __4.len + __5.len +
        __6.len + __7.len + __8.len +
        __9.len)
  }
  def default(implicit alloc: Allocator) = apply("".getBytes)
}
