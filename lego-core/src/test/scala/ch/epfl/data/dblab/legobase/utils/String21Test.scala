package ch.epfl.data
package dblab.legobase
package utils

import org.scalatest._
import offheap._
import Matchers._

class String21Test extends FlatSpec {
  val alphabet = "abcdefghijklmnopqrstuvwxyz".getBytes

  "String21" should "have the correct length" in {
    val s1 = String21("abc".getBytes, 0, 3)
    s1.len should be(3)

    val s2 = String21("".getBytes, 0, 0)
    s2.len should be(0)

    val s3 = String21("abcabcabcabcabcabcabc".getBytes, 0, 21)
    s3.len should be(21)
  }

  it should "when empty, be equal to another empty String21" in {
    val s1 = String21("abc".getBytes, 0, 0)
    val s2 = String21("def".getBytes, 0, 0)
    s1 === s2 should be(true)
  }

  it should "behave correctly when comparing two varying String21 " in {
    val s1 = String21("abcdefghijkl".getBytes, 1, 5)
    val s2 = String21("zyxqrst".getBytes, 0, 4)
    s1 =!= s2 should be(true)
  }

  it should "behave correctly when comparing two equal String21" in {
    val s1 = String21("abcdefghijkl".getBytes, 3, 5)
    val s2 = String21("abcdefghijkl".getBytes, 3, 5)
    val s3 = String21("zydefghabcd".getBytes, 2, 5)
    s1 === s3 should be(true)
    s1 === s3 should be(true)
  }

  it should "correctly implement the === and =!= operator" in {
    val s1 = String21("abc".getBytes, 0, 3)
    val s2 = String21("def".getBytes, 0, 3)
    val s3 = String21("xyzabc123".getBytes, 3, 3)
    s1 === s2 should be(false)
    s1 =!= s2 should be(true)
    s1 === s3 should be(true)
    s1 =!= s3 should be(false)
  }

  it should "iterate through all the elements" in {
    val s1 = String21("abcdef".getBytes, 0, 3)
    val iter = s1.iterator
    iter.next should be('a'.toByte)
    iter.next should be('b'.toByte)
    iter.next should be('c'.toByte)
    iter.hasNext should be(false)
  }

  it should "iterate through no element when the length is 0" in {
    val s1 = String21("12345".getBytes, 0, 0)
    val iter = s1.iterator
    iter.hasNext should be(false)
    intercept[NoSuchElementException] {
      iter.next
    }
  }

  it should "not iterate after the Iterator has been exhausted" in {
    val s1 = String21("abcdef".getBytes, 0, 3)
    val iter = s1.iterator
    iter.next
    iter.next
    iter.next
    iter.hasNext should be(false)
    intercept[NoSuchElementException] {
      iter.next
    }
  }

  it should "not return a value at a position greater than the length or smaller than 0" in {
    val s1 = String21("xyzabc123".getBytes, 3, 3)
    intercept[NoSuchElementException] {
      s1(-1)
    }
    s1(2) should be('c'.toByte)
    intercept[NoSuchElementException] {
      s1(3)
    }
  }

  it should "be filled with the correct offset bytes from the data array" in {
    val s1 = String21(alphabet, 10, 10)
    val s2 = String21("klmnopqrst".getBytes, 0, 10)
    s1 === s2 should be(true)
  }

  it should "refuse to be filled with more than 21 bytes" in {
    intercept[IllegalArgumentException] {
      String21(alphabet, 0, 26)
    }
  }

  it should "return the string with the string method" in {
    val s1 = String21("klmnopqrst".getBytes, 0, 10)
    s1.string should be("klmnopqrst")
  }
}