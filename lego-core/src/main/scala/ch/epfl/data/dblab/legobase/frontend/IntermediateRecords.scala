package ch.epfl.data
package dblab.legobase
package frontend

import scala.reflect.runtime.universe._
import sc.pardis.shallow.CaseClassRecord

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

case class IntermediateRecord4[A: TypeTag, B: TypeTag, C: TypeTag, D: TypeTag](arg1: A, arg2: B, arg3: C, arg4: D)(names: Seq[String]) extends IntermediateRecord {
  def getNameIndex(name: String) = names.indexOf(name) match {
    case 0 => Some("arg1")
    case 1 => Some("arg2")
    case 2 => Some("arg3")
    case 3 => Some("arg4")
    case _ => None
  }
}

case class IntermediateRecord5[A: TypeTag, B: TypeTag, C: TypeTag, D: TypeTag, E: TypeTag](arg1: A, arg2: B, arg3: C, arg4: D, arg5: E)(names: Seq[String]) extends IntermediateRecord {
  def getNameIndex(name: String) = names.indexOf(name) match {
    case 0 => Some("arg1")
    case 1 => Some("arg2")
    case 2 => Some("arg3")
    case 3 => Some("arg4")
    case 4 => Some("arg5")
    case _ => None
  }
}

case class IntermediateRecord7[A: TypeTag, B: TypeTag, C: TypeTag, D: TypeTag, E: TypeTag, F: TypeTag, G: TypeTag](arg1: A, arg2: B, arg3: C, arg4: D, arg5: E, arg6: F, arg7: G)(names: Seq[String]) extends IntermediateRecord {
  def getNameIndex(name: String) = names.indexOf(name) match {
    case 0 => Some("arg1")
    case 1 => Some("arg2")
    case 2 => Some("arg3")
    case 3 => Some("arg4")
    case 4 => Some("arg5")
    case 5 => Some("arg6")
    case 6 => Some("arg7")
    case _ => None
  }
}