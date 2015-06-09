package ch.epfl.data
package dblab
package offheap

// import sc.pardis.shallow.OptimalString
import org.scalatest._
import scala.offheap._
import Matchers._

class OffheapStringTest extends FlatSpec with Matchers {
  val alphabet = "abcdefghijklmnopqrstuvwxyz".getBytes

  "OffheapString" should "have the correct length" in {
    val t1 = OffheapString("abcde".getBytes)
    t1.length should be(5)

    val t2 = OffheapString("".getBytes)
    t2.length should be(0)

    val t3 = OffheapString(alphabet)
    t3.length should be(26)
  }

  it should "refuse to be filled with a string longer than 210 characters" in {
    OffheapString(("x" * 210).getBytes)
    intercept[IllegalArgumentException] {
      OffheapString(("x" * 211).getBytes)
    }
  }

  it should "when empty, be equal to another empty OffheapString" in {
    val t1 = OffheapString("".getBytes)
    val t2 = OffheapString("".getBytes)
    t1 === t2 should be(true)
  }

  it should "behave correctly when comparing two varying OffheapStrings " in {
    val t1 = OffheapString("abc".getBytes)
    val t2 = OffheapString("def".getBytes)
    t1 =!= t2 should be(true)
  }

  it should "behave correctly when comparing two equal OffheapStrings" in {
    val t1 = OffheapString(alphabet)
    val t2 = OffheapString(alphabet)
    t1 === t2 should be(true)
  }

  it should "iterate through all the elements of its String21s" in {
    val t1 = OffheapString(("a" * 150).getBytes)
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
    val t1 = OffheapString("".getBytes)
    val iter = t1.iterator
    iter.hasNext should be(false)
    intercept[NoSuchElementException] {
      iter.next
    }
  }

  it should "not iterate after the Iterator has been exhausted" in {
    val t1 = OffheapString("abc".getBytes)
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
    val t1 = OffheapString(alphabet)
    t1.string should be("abcdefghijklmnopqrstuvwxyz")
  }

  it should "correctly implement endsWith with another OffheapString" in {
    val t1 = OffheapString("0123456789".getBytes)
    val t2 = OffheapString("456789".getBytes)
    t1 endsWith t2 should be(true)

    val t3 = OffheapString("345".getBytes)
    t1 endsWith t3 should be(false)

    t2 endsWith t2 should be(true)

    val t4 = OffheapString("45".getBytes)
    t4 endsWith t3 should be(false)

    val t5 = OffheapString("".getBytes)
    t1 endsWith t5 should be(true)

    t5 endsWith t5 should be(true)
  }

  it should "implement diff correctly" in (pending)

  it should "implement apply correctly" in {
    val t1 = OffheapString(alphabet)
    t1(0) should be('a'.toByte)
    t1(1) should be('b'.toByte)
    t1(25) should be('z'.toByte)
    intercept[NoSuchElementException] {
      t1(26)
    }
    intercept[NoSuchElementException] {
      t1(-1)
    }
  }

  it should "implement startsWith correctly" in {
    val t1 = OffheapString("0123456789".getBytes)
    val t2 = OffheapString("01234".getBytes)
    t1 startsWith t2 should be(true)

    val t3 = OffheapString("345".getBytes)
    t1 startsWith t3 should be(false)

    t2 startsWith t2 should be(true)

    val t4 = OffheapString("45".getBytes)
    t4 startsWith t3 should be(false)

    val t5 = OffheapString("".getBytes)
    t1 startsWith t5 should be(true)

    t5 startsWith t5 should be(true)
  }

  it should "implement containsSlice correctly" in {
    val t1 = OffheapString("0123456789".getBytes)
    val t2 = OffheapString("2345".getBytes)
    val t3 = OffheapString("0123456".getBytes)
    val t4 = OffheapString("910".getBytes)
    val t5 = OffheapString("".getBytes)

    t1 containsSlice t2 should be(true)
    t2 containsSlice t1 should be(false)
    t1 containsSlice t1 should be(true)
    t1 containsSlice t3 should be(true)
    t1 containsSlice t4 should be(false)
    t2 containsSlice t5 should be(true)
    t5 containsSlice t5 should be(true)
    t5 containsSlice t4 should be(false)
  }

  it should "implement slice correctly" in {
    val t1 = OffheapString(alphabet)
    t1.slice(5, 10).string should be("fghij")
    t1.slice(20, 21).string should be("u")
    t1.slice(16, 16).string should be("")
    t1.slice(20, 16).string should be("")
    t1.slice(3, -3).string should be("")
    t1.slice(30, 33).string should be("")
  }

  it should "implement indexOfSlice correctly" in {
    val t1 = OffheapString(alphabet)
    val t2 = OffheapString("defghij".getBytes)
    val t3 = OffheapString(alphabet ++ "0123456789".getBytes)
    val t4 = OffheapString("a".getBytes)
    val t5 = OffheapString("".getBytes)
    t1.indexOfSlice(t2, 0) should be(3)
    t1.indexOfSlice(t2, 3) should be(3)
    t1.indexOfSlice(t2, 4) should be(-1)
    t1.indexOfSlice(t2, 100) should be(-1)
    t1.indexOfSlice(t2, -100) should be(3)
    t1.indexOfSlice(t4, 0) should be(0)
    t1.indexOfSlice(t1, 0) should be(0)
    t1.indexOfSlice(t1, 1) should be(-1)
    t1.indexOfSlice(t3, 0) should be(-1)
    t1.indexOfSlice(t5, 0) should be(0)
    t1.indexOfSlice(t5, 2) should be(2)
    t1.indexOfSlice(t5, 100) should be(-1)
    t1.indexOfSlice(t5, -2) should be(0)
  }
}