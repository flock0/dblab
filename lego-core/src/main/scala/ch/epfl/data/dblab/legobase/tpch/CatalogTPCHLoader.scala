package ch.epfl.data
package dblab.legobase
package tpch

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import tpch._
import schema._
import storagemanager._
import sc.pardis.shallow.OptimalString
import sc.pardis.types._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

@needs[(K2DBScanner, Array[_], OptimalString, Loader, Table)]
@deep
trait CatalogTPCHLoader

/**
 * A module that defines loaders for TPCH relations using the Catalog.
 */
object CatalogTPCHLoader {

  @dontLift
  val tpchSchemaName: String = "TPCH"
  @dontLift
  val tpchSchema: Schema = TPCHSchema.getSchema(Config.datapath, getScalingFactor)
  @dontLift
  val catalog: Catalog = Catalog(Map(tpchSchemaName -> tpchSchema))
  @dontInline
  def getTable(tableName: String): Table = tpchSchema.tables.find(t => t.name == tableName).get
  @dontLift
  def getScalingFactor: Double = Config.datapath.slice(Config.datapath.lastIndexOfSlice("sf") + 2, Config.datapath.length - 1).toDouble //TODO Pass SF to Config

  import Loader.loadTable

  def loadRegion() = catalog.getTuples(tpchSchemaName, "REGION")

  def loadPartsupp() = catalog.getTuples(tpchSchemaName, "PARTSUPP")

  def loadPart() = catalog.getTuples(tpchSchemaName, "PART")

  def loadNation() = catalog.getTuples(tpchSchemaName, "NATION")

  def loadSupplier() = catalog.getTuples(tpchSchemaName, "SUPPLIER")

  def loadLineitem() = catalog.getTuples(tpchSchemaName, "LINEITEM")

  def loadOrders() = catalog.getTuples(tpchSchemaName, "ORDERS")

  def loadCustomer() = catalog.getTuples(tpchSchemaName, "CUSTOMER")
}