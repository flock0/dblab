package ch.epfl.data
package dblab

package object offheap {
  implicit val alloc = scala.offheap.malloc
}
