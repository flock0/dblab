/* Generated by Purgatory 2014-2015 */

package ch.epfl.data.dblab.legobase.deep.storagemanager

import ch.epfl.data.sc.pardis
import pardis.ir._
import pardis.types.PardisTypeImplicits._
import pardis.effects._
import pardis.deep._
import pardis.deep.scalalib._
import pardis.deep.scalalib.collection._
import pardis.deep.scalalib.io._
trait K2DBScannerOps extends Base {
  // Type representation
  val K2DBScannerType = K2DBScannerIRs.K2DBScannerType
  implicit val typeK2DBScanner: TypeRep[K2DBScanner] = K2DBScannerType
  implicit class K2DBScannerRep(self: Rep[K2DBScanner]) {
    def next_int(): Rep[Int] = k2DBScannerNext_int(self)
    def next_double(): Rep[Double] = k2DBScannerNext_double(self)
    def next_char(): Rep[Char] = k2DBScannerNext_char(self)
    def next(buf: Rep[Array[Byte]])(implicit overload1: Overloaded1): Rep[Int] = k2DBScannerNext1(self, buf)
    def next(buf: Rep[Array[Byte]], offset: Rep[Int])(implicit overload2: Overloaded2): Rep[Int] = k2DBScannerNext2(self, buf, offset)
    def next_date: Rep[Int] = k2DBScannerNext_date(self)
    def hasNext(): Rep[Boolean] = k2DBScannerHasNext(self)
    def filename: Rep[String] = k2DBScanner_Field_Filename(self)
  }
  object K2DBScanner {

  }
  // constructors
  def __newK2DBScanner(filename: Rep[String]): Rep[K2DBScanner] = k2DBScannerNew(filename)
  // IR defs
  val K2DBScannerNew = K2DBScannerIRs.K2DBScannerNew
  type K2DBScannerNew = K2DBScannerIRs.K2DBScannerNew
  val K2DBScannerNext_int = K2DBScannerIRs.K2DBScannerNext_int
  type K2DBScannerNext_int = K2DBScannerIRs.K2DBScannerNext_int
  val K2DBScannerNext_double = K2DBScannerIRs.K2DBScannerNext_double
  type K2DBScannerNext_double = K2DBScannerIRs.K2DBScannerNext_double
  val K2DBScannerNext_char = K2DBScannerIRs.K2DBScannerNext_char
  type K2DBScannerNext_char = K2DBScannerIRs.K2DBScannerNext_char
  val K2DBScannerNext1 = K2DBScannerIRs.K2DBScannerNext1
  type K2DBScannerNext1 = K2DBScannerIRs.K2DBScannerNext1
  val K2DBScannerNext2 = K2DBScannerIRs.K2DBScannerNext2
  type K2DBScannerNext2 = K2DBScannerIRs.K2DBScannerNext2
  val K2DBScannerNext_date = K2DBScannerIRs.K2DBScannerNext_date
  type K2DBScannerNext_date = K2DBScannerIRs.K2DBScannerNext_date
  val K2DBScannerHasNext = K2DBScannerIRs.K2DBScannerHasNext
  type K2DBScannerHasNext = K2DBScannerIRs.K2DBScannerHasNext
  val K2DBScanner_Field_Filename = K2DBScannerIRs.K2DBScanner_Field_Filename
  type K2DBScanner_Field_Filename = K2DBScannerIRs.K2DBScanner_Field_Filename
  // method definitions
  def k2DBScannerNew(filename: Rep[String]): Rep[K2DBScanner] = K2DBScannerNew(filename)
  def k2DBScannerNext_int(self: Rep[K2DBScanner]): Rep[Int] = K2DBScannerNext_int(self)
  def k2DBScannerNext_double(self: Rep[K2DBScanner]): Rep[Double] = K2DBScannerNext_double(self)
  def k2DBScannerNext_char(self: Rep[K2DBScanner]): Rep[Char] = K2DBScannerNext_char(self)
  def k2DBScannerNext1(self: Rep[K2DBScanner], buf: Rep[Array[Byte]]): Rep[Int] = K2DBScannerNext1(self, buf)
  def k2DBScannerNext2(self: Rep[K2DBScanner], buf: Rep[Array[Byte]], offset: Rep[Int]): Rep[Int] = K2DBScannerNext2(self, buf, offset)
  def k2DBScannerNext_date(self: Rep[K2DBScanner]): Rep[Int] = K2DBScannerNext_date(self)
  def k2DBScannerHasNext(self: Rep[K2DBScanner]): Rep[Boolean] = K2DBScannerHasNext(self)
  def k2DBScanner_Field_Filename(self: Rep[K2DBScanner]): Rep[String] = K2DBScanner_Field_Filename(self)
  type K2DBScanner = ch.epfl.data.dblab.legobase.storagemanager.K2DBScanner
}
object K2DBScannerIRs extends Base {
  // Type representation
  case object K2DBScannerType extends TypeRep[K2DBScanner] {
    def rebuild(newArguments: TypeRep[_]*): TypeRep[_] = K2DBScannerType
    val name = "K2DBScanner"
    val typeArguments = Nil

    val typeTag = scala.reflect.runtime.universe.typeTag[K2DBScanner]
  }
  implicit val typeK2DBScanner: TypeRep[K2DBScanner] = K2DBScannerType
  // case classes
  case class K2DBScannerNew(filename: Rep[String]) extends ConstructorDef[K2DBScanner](List(), "K2DBScanner", List(List(filename))) {
    override def curriedConstructor = (copy _)
  }

  case class K2DBScannerNext_int(self: Rep[K2DBScanner]) extends FunctionDef[Int](Some(self), "next_int", List(List())) {
    override def curriedConstructor = (copy _)
  }

  case class K2DBScannerNext_double(self: Rep[K2DBScanner]) extends FunctionDef[Double](Some(self), "next_double", List(List())) {
    override def curriedConstructor = (copy _)
  }

  case class K2DBScannerNext_char(self: Rep[K2DBScanner]) extends FunctionDef[Char](Some(self), "next_char", List(List())) {
    override def curriedConstructor = (copy _)
  }

  case class K2DBScannerNext1(self: Rep[K2DBScanner], buf: Rep[Array[Byte]]) extends FunctionDef[Int](Some(self), "next", List(List(buf))) {
    override def curriedConstructor = (copy _).curried
  }

  case class K2DBScannerNext2(self: Rep[K2DBScanner], buf: Rep[Array[Byte]], offset: Rep[Int]) extends FunctionDef[Int](Some(self), "next", List(List(buf, offset))) {
    override def curriedConstructor = (copy _).curried
  }

  case class K2DBScannerNext_date(self: Rep[K2DBScanner]) extends FunctionDef[Int](Some(self), "next_date", List()) {
    override def curriedConstructor = (copy _)
  }

  case class K2DBScannerHasNext(self: Rep[K2DBScanner]) extends FunctionDef[Boolean](Some(self), "hasNext", List(List())) {
    override def curriedConstructor = (copy _)
  }

  case class K2DBScanner_Field_Filename(self: Rep[K2DBScanner]) extends FieldDef[String](self, "filename") {
    override def curriedConstructor = (copy _)
    override def isPure = true

  }

  type K2DBScanner = ch.epfl.data.dblab.legobase.storagemanager.K2DBScanner
}
trait K2DBScannerImplicits extends K2DBScannerOps {
  // Add implicit conversions here!
}
trait K2DBScannerPartialEvaluation extends K2DBScannerComponent with BasePartialEvaluation {
  // Immutable field inlining 
  override def k2DBScanner_Field_Filename(self: Rep[K2DBScanner]): Rep[String] = self match {
    case Def(node: K2DBScannerNew) => node.filename
    case _                         => super.k2DBScanner_Field_Filename(self)
  }

  // Mutable field inlining 
  // Pure function partial evaluation
}
trait K2DBScannerComponent extends K2DBScannerOps with K2DBScannerImplicits {}
