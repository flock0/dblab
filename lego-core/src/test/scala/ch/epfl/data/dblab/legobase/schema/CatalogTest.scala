package ch.epfl.data
package dblab.legobase
package schema

import ch.epfl.data.dblab.legobase.storagemanager._
import ch.epfl.data.dblab.legobase.LBString
import org.scalatest._
import datadict.DDCatalog
import map.MapCatalog

class CatalogTest extends FlatSpec with BeforeAndAfterEach {

  override def beforeEach() {
    addTPCHSchema(cat)
  }

  // Close and delete the temp file
  override def afterEach() {
    val schema = cat.findSchema("TPCH")
    val tables = schema.tables
    tables.foreach(schema.dropTable(_.name))
  }

  val DATAPATH = System.getenv("LEGO_DATA_FOLDER")
  if (DATAPATH != null) {
    val ddCat = DDCatalog
    executeTests(DDCatalog, "DDCatalog")
    executeTests(MapCatalog, "MapCatalog")
  } else {
    fail("Tests could not run because the environment variable `LEGO_DATA_FOLDER` does not exist.")
  }

  def executeTests(cat: Catalog, catName: String) = {
    catName should "add TPCH schema correctly" in {
      // TPCH schema is added in the beforeEach method
      val schema = cat.findSchema("TPCH")
      val tables = schema.tables
      assert(tables.size == 8)

      val tableNames = tables.map(_.name)
      assert(tableNames.fold(true)(_ && Seq("LINEITEM", "ORDERS", "CUSTOMER", "SUPPLIER",
        "PARTSUPP", "REGION", "NATION", "PART") contains _))

      val regionTable = schema.findTable("REGION")
      assert(regionTable.attributes.size == 3)
      )
      val regKeyAttr = regionTable.findAttribute("R_REGIONKEY")
      assert(regKeyAttr.name == "R_REGIONKEY")
      assert(regKeyAttr.dataType == IntType)
    }

    it should "drop tables correctly" in {
      val schema = cat.findSchema("TPCH")
      val tables = schema.tables
      tables.foreach(schema.dropTable(_.name))
      assert(cat.findSchema("TPCH").schema.tables.size == 0)
    }

    it should "handle constraints from the TPCH schema correctly" in {
      val schema = cat.findSchema("TPCH")
      val partTable = schema.findTable("PART")
      val constraints = partTable.constraints

      assert(constraints.size == 14)
      val partKeyAttr = partTable.findAttribute("P_PARTKEY")
      assert(partKeyAttr.hasConstraint(PrimaryKey(Seq(partKeyAttr))))
      assert(partKeyAttr.hasConstraint(NotNull(partKeyAttr)))

      val pkName = partTable.primaryKey match {
        case Some(PrimaryKey(Seq(at))) => at.name
        case None => fail("PART table should have a primary key at this point")
      }
      assert(pkName == "P_PARTKEY")

      partTable.dropPrimaryKey
      partTable.primaryKey match {
        case None =>
        case _ => fail("PART table should not have a primary key as it has been dropped")
      }

      partTable.addConstraint(PrimaryKey(Seq(partKeyAttr)))

      val pkName2 = partTable.primaryKey match {
        case Some(PrimaryKey(Seq(at))) => at.name
        case None => fail("PART table should have a primary key at this point")
      }
      assert(pkName2 == "P_PARTKEY")
    }

    it should "reference foreign tables with ForeignKey constraints correctly" in {
      implicit val schema = cat.findSchema("TPCH")
      val nationTable = schema.findTable("NATION")
      val fk = nationTable.foreignKey("NATION_FK1") match {
        case Some(f : ForeignKey) => f
        case _ => fail("NATION table should have a foreign key")
      }

      assert(fk.thisTable.name == "REGION")
      assert(fk.foreignTable.name == "REGION")
    }
  }
}
