package ch.epfl.data
package dblab.legobase
package schema.map

import sc.pardis.types._
import scala.language.implicitConversions
import scala.collection.mutable.ListMap
import scala.collection.mutable.ArrayBuffer
import schema._
import tpch._
import storagemanager.Loader

import scala.reflect._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.currentMirror

object MapCatalog extends Catalog {
  val schemata = new scala.collection.mutable.HashMap[String, MapSchema]()
  override def findSchema(name: String): Schema = schemata.get(name) match {
    case Some(s) => s
    case None    => throw new Exception("Schema " + name + " not found in catalog!")
  }
  override def getSchemaOrElseCreate(name: String): Schema = schemata.getOrElseUpdate(name, new MapSchema())
  override def createAttribute(name: String, dataType: Tpe, constraints: Seq[Constraint]): Attribute = MapAttribute(name, dataType, constraints)
}
case class MapSchema(tables: ArrayBuffer[Table] = ArrayBuffer(), stats: Statistics = Statistics()) extends Schema {
  override def findTable(name: String) = tables.find(t => t.name == name) match {
    case Some(tab) => tab
    case None      => throw new Exception("Table " + name + " not found in schema!")
  }
  override def findAttribute(attrName: String): Option[Attribute] = tables.map(t => t.findAttribute(attrName)).flatten.toList match {
    case List() => None //throw new Exception("Attribute " + attrName + " not found in schema!")
    case l      => Some(l.apply(0)) // todo -- OK, but assumes that all attribute names are unique
  }
  override def addTable(name: String, attributes: Seq[(String, PardisType[_], List[schema.Compressed.type])], fileName: String, rowCount: Long) =
    tables += MapTable(name, attributes, ArrayBuffer(), fileName, rowCount)
  override def dropTable(tableName: String): Unit = tables.find(_.name == tableName) match {
    case Some(t) => tables -= t
    case None    =>
  }
  override def toString() = {
    val schemaName = MapCatalog.schemata.find({ case (k, v) => v.equals(this) }).get._1
    val header = "\nSchema " + schemaName + "\n" + "-" * (schemaName.length + 8) + "\n\n"
    tables.foldLeft(header)((str, t) => {
      str + t.name + " (" + t.attributes.mkString("\n", "\n", "\n") + ");\n" + t.constraints.mkString("\n") + "\n\n"
    })
  }
}
case class MapTable(name: String, attributes: Seq[Attribute], constraints: ArrayBuffer[Constraint], fileName: String, var rowCount: Long) extends Table {
  override def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  override def dropPrimaryKey = primaryKey match {
    case Some(pk) => constraints -= pk
    case None     => System.out.println(s"${scala.Console.YELLOW}Warning${scala.Console.RESET}: Primary Key for table " + name + " cannot be dropped as it does not exist!")
  }
  override def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }.toList
  override def foreignKey(foreignKeyName: String): Option[ForeignKey] = foreignKeys.find(f => f.foreignKeyName == foreignKeyName)
  override def dropForeignKey(foreignKeyName: String) = foreignKey(foreignKeyName) match {
    case Some(fk) => constraints -= fk
    case None     => System.out.println(s"${scala.Console.YELLOW}Warning${scala.Console.RESET}: ForeignKey Key " + foreignKeyName + " for table " + name + " cannot be dropped as it does not exist!")
  }
  override def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }.toList
  override def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }.toList
  override def autoIncrement: Option[AutoIncrement] = constraints.collectFirst { case ainc: AutoIncrement => ainc }
  override def findAttribute(attrName: String): Option[Attribute] = attributes.find(attr => attr.name == attrName)
  override def addConstraint(cstr: Constraint) = constraints += cstr
  override def load: Array[_] = name match {
    // TODO: Generalize -- make this tpch agnostic
    case "LINEITEM" => Loader.loadTable[LINEITEMRecord](this)(classTag[LINEITEMRecord])
    case "CUSTOMER" => Loader.loadTable[CUSTOMERRecord](this)(classTag[CUSTOMERRecord])
    case "ORDERS"   => Loader.loadTable[ORDERSRecord](this)(classTag[ORDERSRecord])
    case "REGION"   => Loader.loadTable[REGIONRecord](this)(classTag[REGIONRecord])
    case "NATION"   => Loader.loadTable[NATIONRecord](this)(classTag[NATIONRecord])
    case "SUPPLIER" => Loader.loadTable[SUPPLIERRecord](this)(classTag[SUPPLIERRecord])
    case "PART"     => Loader.loadTable[PARTRecord](this)(classTag[PARTRecord])
    case "PARTSUPP" => Loader.loadTable[PARTSUPPRecord](this)(classTag[PARTSUPPRecord])
  }
}
case class MapAttribute(name: String, dataType: Tpe, constraints: Seq[Constraint] = List()) extends Attribute {
  override def hasConstraint(con: Constraint) = constraints.contains(con)
  override def toString() = {
    "    " + "%-20s".format(name) + "%-20s".format(dataType) + constraints.map(c => "@%-10s".format(c)).mkString(" , ")
  }
}