package ch.epfl.data
package dblab.legobase

/**
 * The package object for the schema package which decides
 * the catalog implementation to use
 */
package object schema {
  val CurrCatalog: Catalog = schema.datadict.Catalog
}
