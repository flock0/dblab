package ch.epfl.data
package dblab.legobase
package optimization

import scala.language.implicitConversions
import sc.pardis.ir._
// import reflect.runtime.universe.{ TypeTag, Type }
// import sc.pardis.optimization._
// import deep._
import sc.pardis.types._
import sc.pardis.types.PardisTypeImplicits._
import sc.pardis.shallow.utils.DefaultValue

class OffHeapAnalyser {

  val offheapStructs = scala.collection.mutable.ArrayBuffer[PardisStructDef[_]]()

  def isOffheap(tpe: PardisType[_]): Boolean =
    offheapStructs.exists(_.tag.typeName == tpe.name) || tpe.name == "OptimalString"

  def isOffheap(structDef: PardisStructDef[_]): Boolean =
    offheapStructs.contains(structDef)

  def analyse(program: PardisProgram): Unit = {
    var workListFinished = false
    while (!workListFinished) {
      workListFinished = true
      for (structDef <- program.structs) {
        if (!isOffheap(structDef)) {
          val isOffHeap = structDef.fields.forall(f => !f.tpe.isArray && (f.tpe.isPrimitive || isOffheap(f.tpe)))
          if (isOffHeap) {
            offheapStructs += structDef
            workListFinished = false
          }
        }
      }
    }
  }
}
