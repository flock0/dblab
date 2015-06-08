package ch.epfl.data
package dblab

package object offheap {
  implicit val alloc = _root_.offheap.malloc
}
