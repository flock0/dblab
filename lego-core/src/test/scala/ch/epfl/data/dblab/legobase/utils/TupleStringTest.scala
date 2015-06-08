package ch.epfl.data
package dblab.legobase
package utils

import sc.pardis.shallow.OptimalString
import org.scalatest._
import offheap._
import Matchers._

class TupleStringTest extends FlatSpec with Matchers {
  val alphabet = "abcdefghijklmnopqrstuvwxyz".getBytes

  "TupleString" should "have the correct length" in {
    val t1 = TupleString("abcde".getBytes)
    t1.length should be(5)

    val t2 = TupleString("".getBytes)
    t2.length should be(0)

    val t3 = TupleString(alphabet)
    t3.length should be(26)
  }

  it should "refuse to be filled with a string longer than 210 characters" in {
    TupleString(("x" * 210).getBytes)
    intercept[IllegalArgumentException] {
      TupleString(("x" * 211).getBytes)
    }
  }

  it should "when empty, be equal to another empty TupleString" in {
    val t1 = TupleString("".getBytes)
    val t2 = TupleString("".getBytes)
    t1 === t2 should be(true)
  }

  it should "behave correctly when comparing two varying TupleStrings " in {
    val t1 = TupleString("abc".getBytes)
    val t2 = TupleString("def".getBytes)
    t1 =!= t2 should be(true)
  }

  it should "behave correctly when comparing two equal TupleStrings" in {
    val t1 = TupleString(alphabet)
    val t2 = TupleString(alphabet)
    t1 === t2 should be(true)
  }

  it should "behave correctly when comparing to an OptimalString" in {
    val t1 = TupleString(alphabet)
    val o1 = OptimalString(alphabet)
    t1 === o1 should be(true)
  }

  it should "iterate through all the elements of its String21s" in {
    val t1 = TupleString(("a" * 150).getBytes)
    val iter = t1.iterator
    iter.hasNext should be(true)
    var loopCounter = 0
    for (b <- iter) {
      b should be('a'.toByte)
      loopCounter += 1
    }
    loopCounter should be(150)
    iter.hasNext should be(false)
  }

  it should "iterate through no element when the length is 0" in {
    val t1 = TupleString("".getBytes)
    val iter = t1.iterator
    iter.hasNext should be(false)
    intercept[NoSuchElementException] {
      iter.next
    }
  }

  it should "not iterate after the Iterator has been exhausted" in {
    val t1 = TupleString("abc".getBytes)
    val iter = t1.iterator
    iter.next
    iter.next
    iter.next
    iter.hasNext should be(false)
    intercept[NoSuchElementException] {
      iter.next
    }
  }

  it should "return the string with the string method" in {
    val t1 = TupleString(alphabet)
    t1.string should be("abcdefghijklmnopqrstuvwxyz")
  }

  it should "correctly implement endsWith with another TupleString" in {
    val t1 = TupleString("0123456789".getBytes)
    val t2 = TupleString("456789".getBytes)
    t1 endsWith t2 should be(true)

    val t3 = TupleString("345".getBytes)
    t1 endsWith t3 should be(false)

    t2 endsWith t2 should be(true)

    val t4 = TupleString("45".getBytes)
    t4 endsWith t3 should be(false)

    val t5 = TupleString("".getBytes)
    t1 endsWith t5 should be(true)

    t5 endsWith t5 should be(true)
  }

  it should "correctly implement endsWith with an OptimalString" in {
    val t1 = TupleString("0123456789".getBytes)
    val o1 = OptimalString("456789".getBytes)
    t1 endsWith o1 should be(true)

    val o2 = OptimalString("345".getBytes)
    t1 endsWith o2 should be(false)

    val o3 = OptimalString("".getBytes)
    t1 endsWith o3 should be(true)
  }

  it should "implement diff correctly" in (pending)

  it should "implement apply correctly" in (pending)

  it should "implement startsWith correctly" in (pending)

  it should "implement containsSlice correctly" in (pending)

  it should "implement slice correctly" in (pending)

  it should "implement indexOfSlice correctly" in (pending)

  it should "implement zip correctly" in (pending)

  it should "implement foldLeft correctly" in (pending)

  it should "implement reverse correctly" in (pending)

  it should "implement split correctly" in (pending)
}