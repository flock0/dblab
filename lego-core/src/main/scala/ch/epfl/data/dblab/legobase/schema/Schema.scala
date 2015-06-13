package ch.epfl.data
package dblab.legobase
package schema

import sc.pardis.types._
import scala.language.implicitConversions
import scala.collection.mutable.ListMap
import scala.collection.mutable.ArrayBuffer

object Catalog {
  val schemata = new scala.collection.mutable.HashMap[String, Schema]()
}
case class Schema(tables: ArrayBuffer[Table] = ArrayBuffer(), stats: Statistics = Statistics()) {
  def findTable(name: String) = tables.find(t => t.name == name) match {
    case Some(tab) => tab
    case None      => throw new Exception("Table " + name + " not found in schema!")
  }
  def findAttribute(attrName: String): Option[Attribute] = tables.map(t => t.findAttribute(attrName)).flatten.toList match {
    case List() => None //throw new Exception("Attribute " + attrName + " not found in schema!")
    case l      => Some(l.apply(0)) // todo -- OK, but assumes that all attribute names are unique
  }
  override def toString() = {
    val schemaName = Catalog.schemata.find({ case (k, v) => v.equals(this) }).get._1
    val header = "\nSchema " + schemaName + "\n" + "-" * (schemaName.length + 8) + "\n\n"
    tables.foldLeft(header)((str, t) => {
      str + t.name + " (" + t.attributes.mkString("\n", "\n", "\n") + ");\n" + t.constraints.mkString("\n") + "\n\n"
    })
  }
}
case class Table(name: String, attributes: List[Attribute], constraints: ArrayBuffer[Constraint], resourceLocator: String, var rowCount: Long) {
  def primaryKey: Option[PrimaryKey] = constraints.collectFirst { case pk: PrimaryKey => pk }
  def dropPrimaryKey = primaryKey match {
    case Some(pk) => constraints -= pk
    case None     => System.out.println(s"${scala.Console.YELLOW}Warning${scala.Console.RESET}: Primary Key for table " + name + " cannot be dropped as it does not exist!")
  }
  def foreignKeys: List[ForeignKey] = constraints.collect { case fk: ForeignKey => fk }.toList
  def foreignKey(foreignKeyName: String): Option[ForeignKey] = foreignKeys.find(f => f.foreignKeyName == foreignKeyName)
  def dropForeignKey(foreignKeyName: String) = foreignKey(foreignKeyName) match {
    case Some(fk) => constraints -= fk
    case None     => System.out.println(s"${scala.Console.YELLOW}Warning${scala.Console.RESET}: ForeignKey Key " + foreignKeyName + " for table " + name + " cannot be dropped as it does not exist!")
  }
  def notNulls: List[NotNull] = constraints.collect { case nn: NotNull => nn }.toList
  def uniques: List[Unique] = constraints.collect { case unq: Unique => unq }.toList
  def autoIncrement: Option[AutoIncrement] = constraints.collectFirst { case ainc: AutoIncrement => ainc }
  def findAttribute(attrName: String): Option[Attribute] = attributes.find(attr => attr.name == attrName)
}
case class Attribute(name: String, dataType: Tpe, constraints: List[Constraint] = List()) {
  def hasConstraint(con: Constraint) = constraints.contains(con)
  override def toString() = {
    "    " + "%-20s".format(name) + "%-20s".format(dataType) + constraints.map(c => "@%-10s".format(c)).mkString(" , ")
  }
}
object Attribute {
  implicit def tuple2ToAttribute(nameAndType: (String, Tpe)): Attribute = Attribute(nameAndType._1, nameAndType._2)
}

sealed trait Constraint
case class PrimaryKey(attributes: List[Attribute]) extends Constraint {
  override def toString() = "PrimaryKey(" + attributes.map(a => a.name).mkString(",") + ")"
}
case class ForeignKey(foreignKeyName: String, ownTable: String, referencedTable: String, attributes: List[Attribute]) extends Constraint {
  override def toString() = "ForeignKey(" + attributes.map(a => a.name).mkString(",") + ") references " + referencedTable

  def foreignTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == referencedTable)
  def thisTable(implicit s: Schema): Option[Table] = s.tables.find(t => t.name == ownTable)
  //def matchingAttributes(implicit s: Schema): List[(Attribute, Attribute)] = attributes.map { case (localAttr, foreignAttr) => thisTable.get.attributes.find(a => a.name == localAttr).get -> foreignTable.get.attributes.find(a => a.name == foreignAttr).get }
}
case class NotNull(attribute: Attribute) extends Constraint
case class Unique(attribute: Attribute) extends Constraint
case class AutoIncrement(attribute: Attribute) extends Constraint
object Compressed extends Constraint {
  override def toString = "COMPRESSED"
}

// TODO-GEN: Move this to its own file
case class Statistics() {
  private val statsMap = new scala.collection.mutable.HashMap[String, Double]()
  private def format(name: String) = name.replaceAll("Record", "").replaceAll("Type", "").replaceAll("\\(", "_").replaceAll("\\)", "").replaceAll(",", "_").toUpperCase()

  def +=(nameAndValue: (String, Double)) = statsMap += (format(nameAndValue._1) -> nameAndValue._2)
  def +=(name: String, value: Double) = statsMap += (format(name) -> value)
  def mkString(delim: String) = ListMap(statsMap.toSeq.sortBy(_._1): _*).mkString("\n========= STATISTICS =========\n", delim, "\n==============================\n")
  def increase(nameAndValue: (String, Double)) = statsMap += format(nameAndValue._1) -> (statsMap.getOrElse(format(nameAndValue._1), 0.0) + nameAndValue._2)
  def apply(statName: String): Double = statsMap(statName) // TODO-GEN: will die

  def getCardinality(tableName: String) = statsMap.get("CARDINALITY_" + format(tableName)) match {
    case Some(stat) => stat
    case None =>
      // This means that the statistics module has been asked for either a) a table that does not exist
      // in this schema or b) cardinality of an intermediate table that is being scanned over (see TPCH 
      // Q13 for an example about how this may happen). In both cases, we throw a warning message and
      // return the biggest cardinality
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics do not include cardinality information for table " + tableName + ". Returning largest cardinality to compensate. This may lead to degraded performance due to unnecessarily large memory pool allocations.")
      getLargestCardinality()
  }

  def getLargestCardinality() = {
    statsMap.foldLeft(1.0)((max, kv) => {
      if (kv._1.startsWith("CARDINALITY") && kv._2 > max) kv._2
      else max
    })
  }

  // TODO-GEN: The three following functions assume 1-N schemas. We have to make this explicit
  def getJoinOutputEstimation(tableName1: String, tableName2: String): Int = {
    val cardinality1 = getCardinality(tableName1)
    val cardinality2 = getCardinality(tableName2)
    Math.max(cardinality1, cardinality2).toInt
  }
  def getJoinOutputEstimation(intermediateCardinality: Double, tableName2: String): Double = {
    Math.max(intermediateCardinality, getCardinality(tableName2))
  }
  def getJoinOutputEstimation(tableName2: String, intermediateCardinality: Double): Double = {
    Math.max(intermediateCardinality, getCardinality(tableName2))
  }

  def getDistinctAttrValues(attrName: String): Int = statsMap.get("DISTINCT_" + attrName) match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for DISTINCT_" + attrName + " not found. Returning largest cardinality to compensate. This may lead to degraded performance due to unnecessarily large memory pool allocations.")
      getLargestCardinality().toInt // TODO-GEN: Make this return the cardinality of the corresponding table
  }

  def getEstimatedNumObjectsForType(typeName: String) = statsMap("QS_MEM_" + format(typeName))

  def removeQuerySpecificStats() {
    // QS stands for Query specific
    statsMap.retain((k, v) => k.startsWith("QS") == false)
  }

  def getNumYearsAllDates(): Int = statsMap.get("NUM_YEARS_ALL_DATES") match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for NUM_YEARS_ALL_DATES not found. Returning 128 years to compensate.")
      128
  }

  def getOutputSizeEstimation(): Int = statsMap.get("QS_OUTPUT_SIZE_ESTIMATION") match {
    case Some(stat) => stat.toInt
    case None =>
      System.out.println(s"${scala.Console.RED}Warning${scala.Console.RESET}: Statistics value for QS_OUTPUT_SIZE_ESTIMATION not found. Returning largest cardinality to compensate.")
      getLargestCardinality().toInt // This is more than enough for all practical cases encountered so far
  }
}

// TODO add a type representation for fixed-size stringd-sizz