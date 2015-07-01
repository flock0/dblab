package ch.epfl.data
package dblab.legobase
package schema

import scala.language.dynamics

trait DynamicRecord extends sc.pardis.shallow.Record with Dynamic {
  def selectDynamic[T](name: String): T = getField(name) match {
    case Some(v) => v.asInstanceOf[T]
    case None    => throw new Exception(s"Cannot find the field named $name")
  }
}
