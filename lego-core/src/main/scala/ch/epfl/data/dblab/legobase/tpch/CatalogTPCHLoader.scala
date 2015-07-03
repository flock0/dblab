package ch.epfl.data
package dblab.legobase
package tpch

import utils.Utilities._
import sc.pardis.annotations.{ deep, metadeep, dontLift, dontInline, needs }
import queryengine._
import tpch._
import schema.datadict.helper._
import storagemanager._
import sc.pardis.shallow.OptimalString
import sc.pardis.types._
import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror
import ch.epfl.data.dblab.legobase.frontend.DDLInterpreter

@needs[(LegobaseScanner, Array[_], OptimalString, Loader, Table)]
@deep
trait CatalogTPCHLoader

/**
 * A module that defines loaders for TPCH relations.
 */
object CatalogTPCHLoader {

  //@dontLift
  //val tpchSchema: Schema = TPCHSchema.getSchema(Config.datapath, getScalingFactor)
  @dontInline
  def getTable(tableName: String): schema.Table =
    // NEWVERSION (not hardcoded tpch schema -- TODO Must be generalized without DDLInterpreter)
    schema.CurrCatalog.findSchema("TPCH").findTable(tableName)
  // OLDVERSION (when tpchSchema existed)
  // tpchSchema.tables.find(t => t.name == tableName).get
  @dontLift
  def getScalingFactor: Double = Config.datapath.slice(Config.datapath.lastIndexOfSlice("sf") + 2, Config.datapath.length - 1).toDouble //TODO Pass SF to Config

  import Loader.loadTable

  def loadRegion() = getTable("REGION").load

  def loadPartsupp() = getTable("PARTSUPP").load

  def loadPart() = getTable("PART").load

  def loadNation() = getTable("NATION").load

  def loadSupplier() = getTable("SUPPLIER").load

  def loadLineitem() = getTable("LINEITEM").load

  def loadOrders() = getTable("ORDERS").load

  def loadCustomer() = getTable("CUSTOMER").load
}